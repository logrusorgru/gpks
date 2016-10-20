/*
protocol buffers (v3) k-v storage, its keep index in memory

    [ memory ]
    index -> pos

    [  drive ]
              4 byte      4 byte         frame
    pos -> [frame size][message size][(message) ... ]

*/
package gpks

import (
	"github.com/golang/protobuf/proto"
	"github.com/logrusorgru/gpks/pb3"

	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"
)

// ErrUnregisteredType is unregistered tpe error
var ErrUnregisteredType = errors.New("gpks.TypeReg.Get: unregistered type")

// TypeRegister - type register
type TypeRegister map[string]reflect.Type

// Set registers new type, it will panic if argument is not a pointer
func (t TypeRegister) Set(i interface{}) {
	if reflect.ValueOf(i).Kind() != reflect.Ptr {
		panic(errors.New("gpks.TypeRegister.Set() argument must to be a pointer"))
	}
	t[reflect.TypeOf(i).String()] = reflect.TypeOf(i)
}

// Get element of type, if no one - err will be ErrUnregisteredType
func (t TypeRegister) Get(name string) (interface{}, error) {
	if typ, ok := t[name]; ok {
		return reflect.New(typ.Elem()).Elem().Addr().Interface(), nil
	}
	return nil, ErrUnregisteredType
}

// shared type register
var TypeReg = make(TypeRegister)

// Gpks is storage
type Gpks struct {
	sid   map[string]int64
	nid   map[int64]int64
	file  *os.File
	path  string
	index string
	frame int
	mutex *sync.Mutex
}

// New returns *Gpks or error
func New(path, index string) (*Gpks, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	inx, err := os.OpenFile(index, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	inx.Close()
	return &Gpks{
		sid:   make(map[string]int64),
		nid:   make(map[int64]int64),
		file:  file,
		path:  path,
		index: index,
		mutex: new(sync.Mutex),
	}, nil
}

// allocation fix
func (g *Gpks) fill_index(fl *os.File) error {
	var rmnd int64 // remainder
	if fi, err := fl.Stat(); err != nil {
		return err
	} else {
		rmnd = fi.Size()
	}
	bsz := make([]byte, 4)
	for {
		n, err := fl.Read(bsz)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n != 4 {
			return fmt.Errorf(`gpks.Open[fill_index]:
	size reading error
	wrong bytes count
	expected 4, got %d`, n)
		}
		rmnd -= 4
		sz := bytes_to_int(bsz)
		if sz64 := int64(sz); sz64 > rmnd {
			return fmt.Errorf("gpks.Open[fill_index]: message size %d greater than remainder %d", sz, rmnd)
		} else {
			rmnd -= sz64
		}
		m := make([]byte, sz)
		n, err = fl.Read(m)
		if err != nil {
			return err
		}
		if n != sz {
			return fmt.Errorf(`gpks.Open[fill_index]:
	size reading error
	wrong bytes count
	expected %d, got %d`, sz, n)
		}
		el := new(pb3.Element)
		err = proto.Unmarshal(m, el)
		if err != nil {
			return err
		}
		if el.Wtf { // use sid
			g.sid[el.Sid] = el.Pos
		} else {
			g.nid[el.Nid] = el.Pos
		}
	}
	return nil
}

// open existed
func Open(path, index string) (*Gpks, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	indexf, err := os.Open(index)
	if err != nil {
		return nil, err
	}
	defer indexf.Close()
	gp := &Gpks{
		sid:   make(map[string]int64),
		nid:   make(map[int64]int64),
		file:  file,
		path:  path,
		index: index,
	}
	if err = gp.fill_index(indexf); err != nil {
		return nil, err
	}
	return gp, nil
}

// Backup - create copy of index by the file path
func (g *Gpks) Backup(index string) error {
	fl, err := os.OpenFile(index, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fl.Close()
	for id, off := range g.sid {
		el := &pb3.Element{
			Wtf: true, // string index
			Sid: id,   // Nid: 0
			Pos: off,
		}
		bt, err := proto.Marshal(el)
		if err != nil {
			return err
		}
		_, err = fl.Write(int_to_bytes(len(bt)))
		if err != nil {
			return err
		}
		_, err = fl.Write(bt)
		if err != nil {
			return err
		}
	}
	for id, off := range g.nid {
		el := &pb3.Element{
			Wtf: false, // int64 index
			Nid: id,    // Sid: ""
			Pos: off,
		}
		bt, err := proto.Marshal(el)
		if err != nil {
			return err
		}
		_, err = fl.Write(int_to_bytes(len(bt)))
		if err != nil {
			return err
		}
		_, err = fl.Write(bt)
		if err != nil {
			return err
		}
	}
	return nil
}

// Save index
func (g *Gpks) Save() error {
	return g.Backup(g.index)
}

// convert int to []byte
func int_to_bytes(s int) []byte {
	t := uint32(s)
	bt := make([]byte, 4)
	bt[0] = byte(t)
	bt[1] = byte(t >> 8)
	bt[2] = byte(t >> 16)
	bt[3] = byte(t >> 24)
	return bt
}

// convert []byte to int
func bytes_to_int(bt []byte) int {
	var t uint32
	t = uint32(bt[0]) | uint32(bt[1])<<8 | uint32(bt[2])<<16 | uint32(bt[3])<<24
	return int(t)
}

// convert proto.Message to []byte
func marshal(val proto.Message) ([]byte, error) {
	value, err := proto.Marshal(val)
	if err != nil {
		return nil, err
	}
	any := &pb3.Any{
		TypeUrl: reflect.TypeOf(val).String(),
		Value:   value,
	}
	value, err = proto.Marshal(any)
	return value, err
}

// convert []byte to proto.Message
func unmarshal(bt []byte) (proto.Message, error) {
	any := new(pb3.Any)
	if err := proto.Unmarshal(bt, any); err != nil {
		return nil, err
	}
	typ, err := TypeReg.Get(any.TypeUrl)
	if err != nil {
		return nil, err
	}
	pm := typ.(proto.Message)
	err = proto.Unmarshal(any.Value, pm)
	return pm, err
}

var ErrWrongIdType = errors.New("*gpks.Gpks: wrong type of id")

// Set freame size (it's 0 by default)
func (g *Gpks) Frame(f int) {
	g.frame = f
}

// Set create new k-v or owerwrite existed
func (g *Gpks) Set(id interface{}, val proto.Message) error {
	bt, err := marshal(val)
	if err != nil {
		return err
	}
	var nid int64
	switch tid := id.(type) {
	case string:
		// already exist?
		if off, ok := g.sid[tid]; ok {
			// overwrite or write new
			_, err := g.file.Seek(off, 0)
			if err != nil {
				return err
			}
			fmsz := make([]byte, 8) // 4 + 4
			fsz := bytes_to_int(fmsz[:4])
			if fsz <= len(bt) {
				// write to current offset
				// message size
				msz := int_to_bytes(len(bt))        // 4
				_, err = g.file.WriteAt(msz, off+4) // (off + 4) + 4
				if err != nil {
					return err
				}
				// write message
				_, err = g.file.Write(bt)
				if err != nil {
					return err
				}
				// index just the same
				return nil
			} // else
		}
		// write new, seek end
		off, err := g.file.Seek(0, 2)
		if err != nil {
			return err
		}
		// message & frame size
		msz := len(bt)
		fsz := g.frame
		if fsz < msz {
			fsz = msz
		}
		_, err = g.file.Write( // off + 8
			append(int_to_bytes(fsz), int_to_bytes(msz)...),
		)
		if err != nil {
			return err
		}
		// write message
		_, err = g.file.Write(bt)
		if err != nil {
			return err
		}
		// set index
		g.sid[tid] = off
		return nil
	case int64:
		nid = tid
	case int:
		nid = int64(tid)
	default:
		return ErrWrongIdType
	}
	// already exist?
	if off, ok := g.nid[nid]; ok {
		// overwrite or write new
		_, err := g.file.Seek(off, 0)
		if err != nil {
			return err
		}
		fmsz := make([]byte, 8) // 4 + 4
		fsz := bytes_to_int(fmsz[:4])
		if fsz <= len(bt) {
			// write to current offset
			// message size
			msz := int_to_bytes(len(bt))        // 4
			_, err = g.file.WriteAt(msz, off+4) // (off + 4) + 4
			if err != nil {
				return err
			}
			// write message
			_, err = g.file.Write(bt)
			if err != nil {
				return err
			}
			// index just the same
			return nil
		} // else
	}
	// write new, seek end
	off, err := g.file.Seek(0, 2)
	if err != nil {
		return err
	}
	// message & frame size
	msz := len(bt)
	fsz := g.frame
	if fsz < msz {
		fsz = msz
	}
	_, err = g.file.Write( // off + 8
		append(int_to_bytes(fsz), int_to_bytes(msz)...),
	)
	if err != nil {
		return err
	}
	// write message
	_, err = g.file.Write(bt)
	if err != nil {
		return err
	}
	// set index
	g.nid[nid] = off
	return nil
}

// not exist = nil
//var ErrNotExist = errors.New("*gpks.Gpks: not exist")

func (g *Gpks) Get(id interface{}) (proto.Message, error) {
	var nid int64
	switch tid := id.(type) {
	case string:
		// off
		if off, ok := g.sid[tid]; ok {
			_, err := g.file.Seek(off, 0) // shift
			if err != nil {
				return nil, err
			}
			fmsz := make([]byte, 8)
			_, err = g.file.Read(fmsz)
			if err != nil {
				return nil, err
			}
			msz := bytes_to_int(fmsz[4:])
			bt := make([]byte, msz)
			_, err = g.file.Read(bt)
			if err != nil {
				return nil, err
			}
			return unmarshal(bt)
		}
		// not exist
		return nil, nil
	case int64:
		nid = tid
	case int:
		nid = int64(tid)
	default:
		return nil, ErrWrongIdType
	}
	// off
	if off, ok := g.nid[nid]; ok {
		_, err := g.file.Seek(off, 0) // shift
		if err != nil {
			return nil, err
		}
		fmsz := make([]byte, 8)
		_, err = g.file.Read(fmsz)
		if err != nil {
			return nil, err
		}
		msz := bytes_to_int(fmsz[4:])
		bt := make([]byte, msz)
		_, err = g.file.Read(bt)
		if err != nil {
			return nil, err
		}
		return unmarshal(bt)
	}
	// not exist
	return nil, nil
}

// Exist returns true if id existed
func (g *Gpks) Exist(id interface{}) (ok bool, err error) {
	switch tid := id.(type) {
	case string:
		_, ok = g.sid[tid]
	case int64:
		_, ok = g.nid[tid]
	case int:
		_, ok = g.nid[int64(tid)]
	default:
		err = ErrWrongIdType
	}
	return
}

// Del deletes element
func (g *Gpks) Del(id interface{}) error {
	switch tid := id.(type) {
	case string:
		delete(g.sid, tid)
	case int64:
		delete(g.nid, tid)
	case int:
		delete(g.nid, int64(tid))
	default:
		return ErrWrongIdType
	}
	return nil
}

// Len returns elements count
func (g *Gpks) Len() int {
	return len(g.sid) + len(g.nid)
}

//var StopRange = errors.New("gpks: stop the range")

// Ranges - range over string index
func (g *Gpks) RangeS(fn func(string, proto.Message) error) error {
	if fn == nil {
		return nil
	}
	for id, off := range g.sid {
		_, err := g.file.Seek(off, 0) // shift
		if err != nil {
			return err
		}
		fmsz := make([]byte, 8)
		_, err = g.file.Read(fmsz)
		if err != nil {
			return err
		}
		msz := bytes_to_int(fmsz[4:])
		bt := make([]byte, msz)
		_, err = g.file.Read(bt)
		if err != nil {
			return err
		}
		pm, err := unmarshal(bt)
		if err != nil {
			return err
		}
		if err := fn(id, pm); err != nil {
			return err
		}
	}
	return nil
}

// Ranges - range over int64 index
func (g *Gpks) RangeI(fn func(int64, proto.Message) error) error {
	if fn == nil {
		return nil
	}
	for id, off := range g.nid {
		_, err := g.file.Seek(off, 0) // shift
		if err != nil {
			return err
		}
		fmsz := make([]byte, 8)
		_, err = g.file.Read(fmsz)
		if err != nil {
			return err
		}
		msz := bytes_to_int(fmsz[4:])
		bt := make([]byte, msz)
		_, err = g.file.Read(bt)
		if err != nil {
			return err
		}
		pm, err := unmarshal(bt)
		if err != nil {
			return err
		}
		if err := fn(id, pm); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gpks) Compact() error {
	tb, err := ioutil.TempFile(filepath.Dir(g.path), "gpks_base")
	if err != nil {
		return err
	}
	comp := &Gpks{
		sid:   make(map[string]int64),
		nid:   make(map[int64]int64),
		file:  tb,
		path:  tb.Name(),
		index: g.index,
		mutex: new(sync.Mutex),
	}
	g.mutex.Lock()
	defer g.mutex.Unlock()
	err = g.RangeI(func(k int64, v proto.Message) error {
		return comp.Set(k, v)
	})
	if err != nil {
		return err
	}
	err = g.RangeS(func(k string, v proto.Message) error {
		return comp.Set(k, v)
	})
	if err != nil {
		return err
	}
	err = g.Save()
	if err != nil {
		return err
	}
	// FATAL AREA
	err = g.file.Close()
	if err != nil {
		return err
	}
	err = tb.Close()
	if err != nil {
		return err
	}
	err = os.Rename(g.path, g.path+"-comp.bkp")
	if err != nil {
		return err
	}
	err = os.Rename(comp.path, g.path)
	if err != nil {
		return err
	}
	comp.path = g.path
	comp.index = g.index
	comp.file, err = os.OpenFile(comp.path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	*g = *comp
	// NOT FATAL
	err = os.Remove(g.path + "-comp.bkp")
	if err != nil {
		return err
	}
	return nil
}
