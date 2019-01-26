package bsonex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	gbson "github.com/globalsign/mgo/bson"
)

type Value struct {
	kind  byte
	value []byte
}

func (v Value) Kind() byte {
	return v.kind
}
func (v Value) RawValue() []byte {
	return v.value
}

func (v Value) Uint64() uint64 {
	if len(v.value) == 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(v.value)
}
func (v Value) Int64() int64 {
	return int64(v.Uint64())
}

func (v Value) Uint32() uint32 {
	if len(v.value) == 0 {
		return 0
	}
	return binary.LittleEndian.Uint32(v.value)
}
func (v Value) Int32() int32 {
	return int32(v.Uint32())
}

func (v Value) Float64() float64 {
	if len(v.value) == 0 {
		return 0
	}
	return math.Float64frombits(v.Uint64())
}

func (v Value) Str() string {
	if len(v.value) == 0 {
		return ""
	}
	return string(v.value[4 : len(v.value)-1])
}

type Binary struct {
	Type byte
	Data []byte
}

func (v Value) Binary() Binary {
	if len(v.value) == 0 {
		return Binary{}
	}
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
	if len(v.value) == 0 {
		return false
	}
	return v.value[0] == 0x1
}

func (v Value) Time() time.Time {
	if len(v.value) == 0 {
		return time.Time{}
	}
	ns := v.Int64() * int64(time.Millisecond)
	return time.Unix(ns/1e9, ns%1e9)
}

func (v Value) Regexp() gbson.RegEx {
	if len(v.value) == 0 {
		return gbson.RegEx{}
	}
	i := bytes.IndexByte(v.value, 0x00)
	return gbson.RegEx{
		Pattern: string(v.value[:i]),
		Options: string(v.value[i+1 : len(v.value)-1]),
	}
}

func (v Value) DBPointer() gbson.DBPointer {
	return gbson.DBPointer{
		Namespace: string(v.value[4 : len(v.value)-12]),
		Id:        gbson.ObjectId(v.value[len(v.value)-12:]),
	}
}

func (v Value) MongoTimestamp() gbson.MongoTimestamp {
	return gbson.MongoTimestamp(v.Int64())
}

func (v Value) IsNull() bool {
	return v.kind == 0x0A
}

func (v Value) IsUndefined() bool {
	return v.kind == 0x06
}
func (v Value) IsMinKey() bool {
	return v.kind == 0xFF
}
func (v Value) IsMaxKey() bool {
	return v.kind == 0x7F
}

func (v Value) Type() byte {
	return v.kind
}

func getint(bs []byte) int {
	return int(binary.LittleEndian.Uint32(bs))
}

func (v Value) Value() interface{} {
	switch v.kind {
	case 0x00:
		return nil
	case 0x01:
		return math.Float64frombits(binary.LittleEndian.Uint64(v.value))
	case 0x02:
		return string(v.value[4 : len(v.value)-1])
	case 0x03:
		return BSON(v.value)
	case 0x04:
		return BSON(v.value)
	case 0x05:
		return gbson.Binary{
			Kind: v.value[4],
			Data: v.value[5:],
		}
	case 0x06:
		return gbson.Undefined
	case 0x07:
		return gbson.ObjectId(v.value)
	case 0x08:
		return v.value[0] == 0x1
	case 0x09:
		ns := int64(binary.LittleEndian.Uint64(v.value)) * int64(time.Millisecond)
		return time.Unix(ns/1e9, ns%1e9)
	case 0x0A:
		return nil
	case 0x0B:
		i := bytes.IndexByte(v.value, 0x00)
		return gbson.RegEx{
			Pattern: string(v.value[:i]),
			Options: string(v.value[i+1 : len(v.value)-1]),
		}
	case 0x0C:
		return gbson.DBPointer{
			Namespace: string(v.value[4 : len(v.value)-12]),
			Id:        gbson.ObjectId(v.value[len(v.value)-12:]),
		}
	case 0x0D:
	case 0x10:
		return binary.LittleEndian.Uint32(v.value)
	case 0x11:
	case 0x12:
		return binary.LittleEndian.Uint64(v.value)
	case 0x13:
	case 0xFF:
		return gbson.MinKey
	case 0x7F:
		return gbson.MaxKey
	default:
		panic(fmt.Sprintf("invalid bson type %#v", v.kind))
	}
	return nil
}
