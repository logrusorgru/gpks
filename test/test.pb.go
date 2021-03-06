// Code generated by protoc-gen-go.
// source: test.proto
// DO NOT EDIT!

/*
Package test is a generated protocol buffer package.

It is generated from these files:
	test.proto

It has these top-level messages:
	X
	Y
*/
package test

import proto "github.com/golang/protobuf/proto"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal

type X struct {
	Hello string `protobuf:"bytes,1,opt,name=hello" json:"hello,omitempty"`
	Size  int64  `protobuf:"varint,2,opt,name=size" json:"size,omitempty"`
}

func (m *X) Reset()         { *m = X{} }
func (m *X) String() string { return proto.CompactTextString(m) }
func (*X) ProtoMessage()    {}

type Y struct {
	Em     []string `protobuf:"bytes,1,rep,name=em" json:"em,omitempty"`
	Length string   `protobuf:"bytes,2,opt,name=length" json:"length,omitempty"`
}

func (m *Y) Reset()         { *m = Y{} }
func (m *Y) String() string { return proto.CompactTextString(m) }
func (*Y) ProtoMessage()    {}

func init() {
}
