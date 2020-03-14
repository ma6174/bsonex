package bsonex

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
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

func (v Value) String() string {
	b, err := json.Marshal(v.Value())
	if err != nil {
		return fmt.Sprint(v.Value())
	}
	return string(b)
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
		Namespace: string(v.value[4 : len(v.value)-13]),
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

func (v Value) Binary() Binary {
	if len(v.value) == 0 {
		return Binary{gbson.Binary{}}
	}
	return Binary{gbson.Binary{
		Kind: v.value[4],
		Data: v.value[5:],
	}}
}

func getint(bs []byte) int {
	return int(binary.LittleEndian.Uint32(bs))
}

type Binary struct {
	gbson.Binary
}

func (b Binary) MarshalJSON() (bs []byte, err error) {
	s := base64.StdEncoding.EncodeToString(b.Data)
	return json.Marshal(s)
}

func (v Value) Value() interface{} {
	switch v.kind {
	case 0x01: // 64-bit binary floating point
		return math.Float64frombits(binary.LittleEndian.Uint64(v.value))
	case 0x02: // UTF-8 string
		return string(v.value[4 : len(v.value)-1])
	case 0x03: // Embedded document
		return BSON(v.value).ToValueMap()
	case 0x04: // Array
		return BSON(v.value).toValueArray()
	case 0x05: // Binary data
		return Binary{gbson.Binary{Kind: v.value[4], Data: v.value[5:]}}
	case 0x06: // Undefined (value) — Deprecated
		return gbson.Undefined
	case 0x07: // ObjectId
		return gbson.ObjectId(v.value)
	case 0x08: // Boolean
		return v.value[0] == 0x1
	case 0x09: // UTC datetime
		ns := int64(binary.LittleEndian.Uint64(v.value)) * int64(time.Millisecond)
		return time.Unix(ns/1e9, ns%1e9)
	case 0x0A: // Null value
		return nil
	case 0x0B: // Regular expression
		i := bytes.IndexByte(v.value, 0x00)
		return gbson.RegEx{
			Pattern: string(v.value[:i]),
			Options: string(v.value[i+1 : len(v.value)-1]),
		}
	case 0x0C: // DBPointer — Deprecated
		return gbson.DBPointer{
			Namespace: string(v.value[4 : len(v.value)-12-1]),
			Id:        gbson.ObjectId(v.value[len(v.value)-12:]),
		}
	case 0x0D: // JavaScript code
		panic("not supported")
	case 0x0E: // Symbol. Deprecated
		panic("not supported")
	case 0x0F: // JavaScript code w/ scope
		panic("not supported")
	case 0x10: // 32-bit integer
		return binary.LittleEndian.Uint32(v.value)
	case 0x11: // Timestamp
		return gbson.MongoTimestamp(binary.LittleEndian.Uint64(v.value))
	case 0x12: // 64-bit integer
		return binary.LittleEndian.Uint64(v.value)
	case 0x13: // 128-bit decimal floating point
		panic("not supported")
	case 0xFF: // Min key
		return gbson.MinKey
	case 0x7F: // Max key
		return gbson.MaxKey
	default:
		panic(fmt.Sprintf("invalid bson type %#v", v.kind))
	}
}
