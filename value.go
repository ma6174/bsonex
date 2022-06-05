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

type ValueType = byte

const (
	TypeEmpty       ValueType = iota
	TypeDouble                // 0x01 64-bit binary floating point
	TypeString                // 0x02 UTF-8 string
	TypeDocument              // 0x03 Embedded document
	TypeArray                 // 0x04
	TypeBinary                // 0x05 Binary data
	TypeUndefined             // 0x06 Undefined (value) — Deprecated
	TypeObjectId              // 0x07
	TypeBoolean               // 0x08
	TypeDatetime              // 0x09 UTC datetime
	TypeNull                  // 0x0A
	TypeRegex                 // 0x0B Regular expression
	TypeDBPointer             // 0x0C Deprecated
	TypeJSCode                // 0x0D
	TypeSymbol                // 0x0E Deprecated
	TypeJSCodeScope           // 0x0F Deprecated
	TypeInt32                 // 0x10
	TypeTimestamp             // 0x11
	TypeInt64                 // 0x12
	TypeDecimal128            // 0x13 128-bit decimal floating point
	TypeMinKey      byte      = 0xFF
	TypeMaxKey      byte      = 0x7F
)

type Value struct {
	valueType ValueType
	valueData []byte
}

func (v Value) IsEmpty() bool {
	return v.valueType == TypeEmpty
}

func (v Value) Type() ValueType {
	return v.valueType
}

func (v Value) RawValue() []byte {
	return v.valueData
}

func (v Value) checkType(expect byte) {
	if expect != v.valueType {
		panic(fmt.Sprintf("invalid type, expect: %v, real: %v",
			expect, v.valueType))
	}
}

func (v Value) checkValueLength(expect int) {
	if expect != len(v.valueData) {
		panic(fmt.Sprintf("invalid length, expect: %v, real: %v",
			expect, len(v.valueData)))
	}
}

func (v Value) Uint64() uint64 {
	if len(v.valueData) == 0 {
		return 0
	}
	if v.valueType == TypeInt32 {
		return uint64(v.Uint32())
	}
	v.checkValueLength(8)
	return binary.LittleEndian.Uint64(v.valueData)
}

func (v Value) Int64() int64 {
	if v.valueType == TypeInt32 {
		return int64(v.Int32())
	}
	return int64(v.Uint64())
}

func (v Value) Uint32() uint32 {
	if len(v.valueData) == 0 {
		return 0
	}
	v.checkType(TypeInt32)
	v.checkValueLength(4)
	return binary.LittleEndian.Uint32(v.valueData)
}

func (v Value) Int32() int32 {
	return int32(v.Uint32())
}

func (v Value) Float64() float64 {
	if len(v.valueData) == 0 {
		return 0
	}
	v.checkType(TypeDouble)
	return math.Float64frombits(v.Uint64())
}

func (v Value) Str() string {
	if len(v.valueData) == 0 {
		return ""
	}
	v.checkType(TypeString)
	return string(v.valueData[4 : len(v.valueData)-1])
}

func (v Value) String() string {
	b, err := json.Marshal(v.Value())
	if err != nil {
		return fmt.Sprint(v.Value())
	}
	return string(b)
}

func (v Value) Document() BSON {
	v.checkType(TypeDocument)
	return BSON(v.valueData)
}

func (v Value) Array() (a []Value) {
	v.checkType(TypeArray)
	return BSON(v.valueData).ToValueArray()
}

func (v Value) ArrayOf(i int) Value {
	v.checkType(TypeArray)
	return BSON(v.valueData).Lookup(strconv.Itoa(i))
}

func (v Value) Objid() gbson.ObjectId {
	v.checkType(TypeObjectId)
	return gbson.ObjectId(v.valueData)
}

func (v Value) Bool() bool {
	if len(v.valueData) == 0 {
		return false
	}
	v.checkType(TypeBoolean)
	return v.valueData[0] == 0x1
}

func (v Value) Time() time.Time {
	if len(v.valueData) == 0 {
		return time.Time{}
	}
	v.checkType(TypeDatetime)
	v.checkValueLength(8)
	ns := v.Int64() * int64(time.Millisecond)
	return time.Unix(ns/1e9, ns%1e9)
}

func (v Value) Regexp() gbson.RegEx {
	if len(v.valueData) == 0 {
		return gbson.RegEx{}
	}
	v.checkType(TypeRegex)
	i := bytes.IndexByte(v.valueData, 0x00)
	return gbson.RegEx{
		Pattern: string(v.valueData[:i]),
		Options: string(v.valueData[i+1 : len(v.valueData)-1]),
	}
}

func (v Value) DBPointer() gbson.DBPointer {
	v.checkType(TypeDBPointer)
	return gbson.DBPointer{
		Namespace: string(v.valueData[4 : len(v.valueData)-13]),
		Id:        gbson.ObjectId(v.valueData[len(v.valueData)-12:]),
	}
}

func (v Value) MongoTimestamp() gbson.MongoTimestamp {
	v.checkType(TypeTimestamp)
	return gbson.MongoTimestamp(v.Int64())
}

func (v Value) IsNull() bool {
	return v.valueType == TypeNull
}

func (v Value) IsUndefined() bool {
	return v.valueType == TypeUndefined
}

func (v Value) IsMinKey() bool {
	return v.valueType == TypeMinKey
}

func (v Value) IsMaxKey() bool {
	return v.valueType == TypeMaxKey
}

func (v Value) Binary() Binary {
	if len(v.valueData) == 0 {
		return Binary{}
	}
	v.checkType(TypeBinary)
	return Binary{gbson.Binary{
		Kind: v.valueData[4],
		Data: v.valueData[5:],
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
	switch v.valueType {
	case TypeDouble:
		return v.Float64()
	case TypeString:
		return v.Str()
	case TypeDocument:
		return v.Document()
	case TypeArray:
		return v.Array()
	case TypeBinary:
		return v.Binary()
	case TypeUndefined:
		return gbson.Undefined
	case TypeObjectId:
		return v.Objid()
	case TypeBoolean:
		return v.Bool()
	case TypeDatetime:
		return v.Time()
	case TypeNull:
		return nil
	case TypeRegex:
		return v.Regexp()
	case TypeDBPointer:
		return v.DBPointer()
	case TypeJSCode, TypeSymbol, TypeJSCodeScope, TypeDecimal128:
		panic("not supported")
	case TypeInt32:
		return v.Uint32()
	case TypeTimestamp:
		return v.MongoTimestamp()
	case TypeInt64:
		return v.Uint64()
	case TypeMinKey:
		return gbson.MinKey
	case TypeMaxKey:
		return gbson.MaxKey
	default:
		panic(fmt.Sprintf("invalid bson type %#v", v.valueType))
	}
}
