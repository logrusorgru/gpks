//
// Copyright (c) 2015 Konstanin Ivanov <kostyarin.ivanov@gmail.com>.
// All rights reserved. This program is free software. It comes without
// any warranty, to the extent permitted by applicable law. You can
// redistribute it and/or modify it under the terms of the Do What
// The Fuck You Want To Public License, Version 2, as published by
// Sam Hocevar. See LICENSE file for more details or see below.
//

//
//        DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
//                    Version 2, December 2004
//
// Copyright (C) 2004 Sam Hocevar <sam@hocevar.net>
//
// Everyone is permitted to copy and distribute verbatim or modified
// copies of this license document, and changing it is allowed as long
// as the name is changed.
//
//            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
//   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
//
//  0. You just DO WHAT THE FUCK YOU WANT TO.
//

package gpks

import (
	"github.com/logrusorgru/gpks/test"

	"fmt"
	"path"
	"reflect"
	"runtime"
	"testing"
)

func init() {
	TypeReg.Set(new(test.X))
	TypeReg.Set(new(test.Y))
}

/*
type X struct {
	Hello string
	Size  int64
}

type Y struct {
	Em     []string
	Length string
}
*/

func four() (a, b *test.X, c, d *test.Y) {
	a = &test.X{
		Hello: "hello gpks 0",
		Size:  0,
	}
	b = &test.X{
		Hello: "hello gpks one",
		Size:  1,
	}
	c = &test.Y{
		Em:     []string{"hello", "gpks", "2"},
		Length: "2",
	}
	d = &test.Y{
		Em:     []string{"hello", "three"},
		Length: "3",
	}
	return
}

func cmp(i, j interface{}) bool {
	if reflect.TypeOf(i) == reflect.TypeOf(j) {
		switch i.(type) {
		case *test.X:
			ix := i.(*test.X)
			jx := j.(*test.X)
			if ix.Hello != jx.Hello {
				return false
			}
			if ix.Size != jx.Size {
				return false
			}
			return true
		case *test.Y:
			iy := i.(*test.Y)
			jy := j.(*test.Y)
			if len(iy.Em) != len(jy.Em) {
				return false
			}
			if iy.Length != jy.Length {
				return false
			}
			for index, _ := range iy.Em {
				if iy.Em[index] != jy.Em[index] {
					return false
				}
			}
			return true
		default:
			fmt.Println("unknown type, r u kidding me?")
			return false
		}
	}
	return false
}

func TestComplexBase(t *testing.T) {
	gp, err := New(path.Join("testee", "db.gpks"), path.Join("testee", "index.gpks"))
	if err != nil {
		t.Error(err)
	}
	if gp.Len() != 0 {
		t.Error("wrong length")
	}
	a, b, c, d := four()
	// set
	if err := gp.Set(0, a); err != nil {
		t.Error(err)
	}
	if gp.Len() != 1 {
		t.Error("wrong length")
	}
	if err := gp.Set("one", b); err != nil {
		t.Error(err)
	}
	if err := gp.Set(2, c); err != nil {
		t.Error(err)
	}
	if err := gp.Set("two", d); err != nil {
		t.Error(err)
	}
	if gp.Len() != 4 {
		t.Error("wrong length")
	}
	// get
	if m, err := gp.Get(0); err != nil {
		t.Error(err)
	} else if !cmp(a, m) {
		t.Errorf("fuck %v != %v", a, m)
	}
	if m, err := gp.Get("one"); err != nil {
		t.Error(err)
	} else if !cmp(b, m) {
		t.Errorf("fuck %v != %v", b, m)
	}
	if m, err := gp.Get(2); err != nil {
		t.Error(err)
	} else if !cmp(c, m) {
		t.Errorf("fuck %v != %v", c, m)
	}
	if m, err := gp.Get("two"); err != nil {
		t.Error(err)
	} else if !cmp(d, m) {
		t.Errorf("fuck %v != %v", d, m)
	}
	// exist
	if ex, err := gp.Exist(0); err != nil {
		t.Error(err)
	} else if !ex {
		t.Errorf("fuck id %v not exist", 0)
	}
	if ex, err := gp.Exist("one"); err != nil {
		t.Error(err)
	} else if !ex {
		t.Errorf("fuck id %v not exist", "one")
	}
	if ex, err := gp.Exist(2); err != nil {
		t.Error(err)
	} else if !ex {
		t.Errorf("fuck id %v not exist", 2)
	}
	if ex, err := gp.Exist("two"); err != nil {
		t.Error(err)
	} else if !ex {
		t.Errorf("fuck id %v not exist", "two")
	}
	// not exist
	if ex, err := gp.Exist(1); err != nil {
		t.Error(err)
	} else if ex {
		t.Errorf("fuck id %v exist", 1)
	}
	if ex, err := gp.Exist("five"); err != nil {
		t.Error(err)
	} else if ex {
		t.Errorf("fuck id %v exist", "five")
	}
	if ex, err := gp.Exist(7); err != nil {
		t.Error(err)
	} else if ex {
		t.Errorf("fuck id %v exist", 7)
	}
	if ex, err := gp.Exist("ten"); err != nil {
		t.Error(err)
	} else if ex {
		t.Errorf("fuck id %v exist", "ten")
	}
	// del
	if err := gp.Del("ten"); err != nil {
		t.Error(err)
	}
	if err := gp.Del("two"); err != nil {
		t.Error(err)
	}
	if ex, err := gp.Exist("two"); err != nil {
		t.Error(err)
	} else if ex {
		t.Errorf("fuck id %v exist", "two")
	}
	if gp.Len() != 3 {
		t.Error("wrong length")
	}
}

func TestOpenSave(t *testing.T) {
	gp, err := New(path.Join("testee", "db.gpks"), path.Join("testee", "index.gpks"))
	if err != nil {
		t.Error(err)
	}
	a, b, c, d := four()
	if err := gp.Set(0, a); err != nil {
		t.Error(err)
	}
	if err := gp.Set("one", b); err != nil {
		t.Error(err)
	}
	if err := gp.Set(2, c); err != nil {
		t.Error(err)
	}
	if err := gp.Set("two", d); err != nil {
		t.Error(err)
	}
	//
	if err := gp.Save(); err != nil {
		t.Error(err)
	}
	gp = nil
	runtime.GC()
	gp, err = Open(path.Join("testee", "db.gpks"), path.Join("testee", "index.gpks"))
	if err != nil {
		t.Error(err)
		return
	}
	if gp.Len() != 4 {
		t.Errorf("wrong length, expected 4, got %d", gp.Len())
	}
	//
	if m, err := gp.Get(0); err != nil {
		t.Error(err)
	} else if !cmp(a, m) {
		t.Errorf("id 0: %v != %v", a, m)
	}
	if m, err := gp.Get("one"); err != nil {
		t.Error(err)
	} else if !cmp(b, m) {
		t.Errorf("id 'one': %v != %v", b, m)
	}
	if m, err := gp.Get(2); err != nil {
		t.Error(err)
	} else if !cmp(c, m) {
		t.Errorf("id 2: %v != %v", c, m)
	}
	if m, err := gp.Get("two"); err != nil {
		t.Error(err)
	} else if !cmp(d, m) {
		t.Errorf("id 'two': %v != %v", d, m)
	}
}

func TestCompact(t *testing.T) {
	gp, err := New(path.Join("testee", "db.gpks"), path.Join("testee", "index.gpks"))
	if err != nil {
		t.Error(err)
	}
	a, b, c, d := four()
	if err := gp.Set(0, a); err != nil {
		t.Error(err)
	}
	if err := gp.Set("one", b); err != nil {
		t.Error(err)
	}
	if err := gp.Set(2, c); err != nil {
		t.Error(err)
	}
	if err := gp.Set("two", d); err != nil {
		t.Error(err)
	}
	//
	if err := gp.Compact(); err != nil {
		t.Error(err)
		return
	}
	//
	if gp.Len() != 4 {
		t.Errorf("wrong length, expected 4, got %d", gp.Len())
	}
	//
	if m, err := gp.Get(0); err != nil {
		t.Error(err)
	} else if !cmp(a, m) {
		t.Errorf("id 0: %v != %v", a, m)
	}
	if m, err := gp.Get("one"); err != nil {
		t.Error(err)
	} else if !cmp(b, m) {
		t.Errorf("id 'one': %v != %v", b, m)
	}
	if m, err := gp.Get(2); err != nil {
		t.Error(err)
	} else if !cmp(c, m) {
		t.Errorf("id 2: %v != %v", c, m)
	}
	if m, err := gp.Get("two"); err != nil {
		t.Error(err)
	} else if !cmp(d, m) {
		t.Errorf("id 'two': %v != %v", d, m)
	}
}
