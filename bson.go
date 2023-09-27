package bsonex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

type BSON []byte

func (b BSON) String() string {
	return string(BSONEX{BSON: b}.MustToJson())
}
func (b BSON) Lookup(key string) (val Value) {
	if key == "" {
		return
	}
	val = Value{valueType: TypeDocument, valueData: b}
	sp := strings.Split(key, ".")
	for _, k := range sp {
		val = BSON(val.valueData).lookupOne(k)
		if val.valueType == TypeEmpty {
			return
		}
	}
	return
}

func (b BSON) lookupOne(key string) (val Value) {
	elements := b[4 : len(b)-1]
	keyb := []byte(key)
	for elements != nil {
		ckey, cval, next := getElement(elements)
		if ckey != nil && bytes.Equal(ckey, keyb) {
			return cval
		}
		elements = next
	}
	return
}

type toSearchValue struct {
	b []byte
}

func NewToSearchValue(v interface{}) (b toSearchValue, err error) {
	if s, ok := v.(string); ok {
		return toSearchValue{[]byte(s)}, nil
	}
	bs, err := Marshal(M{"v": v})
	if err != nil {
		return
	}
	// bson_size(4) + type(1) + key(v,1) + \0 (1) ...value.... \0 (1)
	return toSearchValue{bs[4+1+1+1 : len(bs)-1]}, nil
}

// FastContains 可以在未解析BSON的时候先快速判断一下是否包含待查找的内容，
// 避免执行每个文档都执行Unmarshal加快查找速度。
// 需要注意的是这里查找并不精确，必要的情况下仍然需要再次Unmarshal再确认一次。
func (b BSON) FastContains(v toSearchValue) bool {
	return bytes.Contains(b, v.b)
}

func (b BSON) Map() (vals M) {
	vals = make(M)
	elements := b[4 : len(b)-1]
	for elements != nil {
		ckey, cval, next := getElement(elements)
		if ckey != nil {
			vals[string(ckey)] = cval.Value()
		}
		elements = next
	}
	return
}

func (b BSON) ToValueMap() (vals map[string]Value) {
	vals = make(map[string]Value)
	elements := b[4 : len(b)-1]
	for elements != nil {
		ckey, cval, next := getElement(elements)
		if ckey != nil {
			vals[string(ckey)] = cval
		}
		elements = next
	}
	return
}

func (b BSON) Array() (arr []interface{}) {
	elements := b[4 : len(b)-1]
	for elements != nil {
		ckey, cval, next := getElement(elements)
		if ckey != nil {
			arr = append(arr, cval.Value())
		}
		elements = next
	}
	return
}

func (b BSON) ToValueArray() (arr []Value) {
	elements := b[4 : len(b)-1]
	for elements != nil {
		ckey, cval, next := getElement(elements)
		if ckey != nil {
			arr = append(arr, cval)
		}
		elements = next
	}
	return
}

func (b BSON) Unmarshal(out interface{}) (err error) {
	return Unmarshal(b, out)
}

func (b BSON) ToJson() (s []byte, err error) {
	return json.Marshal(b.Map())
}

func (b BSON) MustToJson() (s []byte) {
	s, err := b.ToJson()
	if err != nil {
		panic(err)
	}
	return
}

func getElement(b BSON) (key []byte, val Value, next BSON) {
	if len(b) == 0 {
		return nil, val, nil
	}
	elementType := b[0]
	keyStart, keyEnd := 1, bytes.IndexByte(b, 0x00)
	key = b[keyStart:keyEnd]
	var valb []byte
	switch elementType {
	case TypeDouble, TypeDatetime, TypeTimestamp, TypeInt64:
		valb = b[keyEnd+1 : keyEnd+1+8]
	case TypeString, TypeJSCode, TypeSymbol, TypeJSCodeScope:
		strLen := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+4+strLen]
	case TypeDocument, TypeArray:
		Len := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+Len]
	case TypeBinary:
		Len := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+4+1+Len]
	case TypeUndefined, TypeNull, TypeMinKey, TypeMaxKey:
		// no value
	case TypeObjectId:
		valb = b[keyEnd+1 : keyEnd+1+12]
	case TypeBoolean:
		valb = b[keyEnd+1 : keyEnd+1+1]
	case TypeRegex:
		i := bytes.IndexByte(b[keyEnd+1:], 0x00)
		i2 := bytes.IndexByte(b[keyEnd+1+i+1:], 0x00)
		valb = b[keyEnd+1 : keyEnd+1+i+1+i2+1]
	case TypeDBPointer:
		strLen := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+4+int(strLen)+12]
	case TypeInt32:
		valb = b[keyEnd+1 : keyEnd+1+4]
	case TypeDecimal128:
		valb = b[keyEnd+1 : keyEnd+1+16]
	default:
		panic(fmt.Sprintf("invalid bson type %#v", elementType))
	}
	val, next = Value{elementType, valb}, b[len(key)+1+len(valb)+1:]
	return
}
