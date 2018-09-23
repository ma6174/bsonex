package bsonex

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"time"

	gbson "github.com/globalsign/mgo/bson"
)

type Value struct {
	typ   byte
	value []byte
}

func (v Value) Uint64() uint64 {
	return binary.LittleEndian.Uint64(v.value)
}
func (v Value) Int64() int64 {
	return int64(v.Uint64())
}

func (v Value) Uint32() uint32 {
	return binary.LittleEndian.Uint32(v.value)
}
func (v Value) Int32() int32 {
	return int32(v.Uint32())
}

func (v Value) Float64() float64 {
	return math.Float64frombits(v.Uint64())
}

func (v Value) String() string {
	return string(v.value[4 : len(v.value)-1])
}

type Binary struct {
	Type byte
	Data []byte
}

func (v Value) Binary() Binary {
	return Binary{
		Type: v.value[4],
		Data: v.value[5:],
	}
}

func (v Value) Document() BSON {
	return BSON(v.value)
}

func (v Value) ArrayOf(i int) Value {
	return v.Document().Lookup(strconv.Itoa(i))
}

func (v Value) Objid() gbson.ObjectId {
	return gbson.ObjectId(v.value)
}

func (v Value) Bool() bool {
	return v.value[0] == 0x1
}

func (v Value) Time() time.Time {
	ns := v.Int64() * int64(time.Millisecond)
	return time.Unix(ns/1e9, ns%1e9)
}

func (v Value) Regexp() (pattern, options string) {
	i := bytes.IndexByte(v.value, 0x00)
	return string(v.value[:i]), string(v.value[i+1 : len(v.value)-1])
}

func (v Value) DBPointer() (namespace string, ID gbson.ObjectId) {
	return string(v.value[4 : len(v.value)-12]), gbson.ObjectId(v.value[len(v.value)-12:])
}

func (v Value) IsNull() bool {
	return v.typ == 0x0A
}

func (v Value) IsUndefined() bool {
	return v.typ == 0x06
}
func (v Value) IsMinKey() bool {
	return v.typ == 0xFF
}
func (v Value) IsMaxKey() bool {
	return v.typ == 0x7F
}

func (v Value) Type() byte {
	return v.typ
}

func getint(bs []byte) int {
	return int(binary.LittleEndian.Uint32(bs))
}
