package bsonex

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	gbson "github.com/globalsign/mgo/bson"
	"github.com/sbunce/bson"
)

type M map[string]interface{}

type BSON []byte

func (b BSON) String() string {
	return string(BSONEX{BSON: b}.MustToJson())
}

type BSONEX struct {
	BSON
	offset   int64
	runnerID int
}

func (b *BSONEX) Offset() int64 {
	return b.offset
}

func (b *BSONEX) RunnerID() int {
	return b.runnerID
}

func (b *BSONEX) Size() int {
	return len(b.BSON)
}

func (b BSONEX) String() string {
	return string(b.MustToJson())
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
	bs, err := gbson.Marshal(gbson.M{"v": v})
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
	return gbson.Unmarshal(b, out)
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

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{bufio.NewReaderSize(r, 4<<20)}
}

type Decoder struct {
	r io.Reader
}

func (d *Decoder) ForEach(f func(b BSONEX) error) (err error) {
	var offset int64
	for {
		one, err := d.ReadOne()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = f(BSONEX{BSON: one, offset: offset})
		offset += int64(len(one))
		if err != nil {
			return err
		}
	}
	return
}

func (d *Decoder) Do(parallel int, f func(b BSONEX) error) (err error) {
	if parallel <= 1 {
		return d.ForEach(f)
	}
	ch := make(chan []*BSONEX, parallel*2)
	errCh := make(chan error, parallel)
	var wg sync.WaitGroup
	wg.Add(parallel)
	defer wg.Wait()
	for i := 0; i < parallel; i++ {
		go func(id int) {
			defer wg.Done()
			for bs := range ch {
				for _, b := range bs {
					b.runnerID = id
					err := f(*b)
					if err != nil {
						errCh <- err
						return
					}
				}
			}
		}(i)
	}
	defer close(ch)
	var bs []*BSONEX
	var offset int64
	for {
		one, err := d.ReadOne()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		bs = append(bs, &BSONEX{BSON: one, offset: offset})
		offset += int64(len(one))
		if len(bs) == 100 {
			select {
			case ch <- bs:
				bs = nil
			case err = <-errCh:
				return err
			}
		}
	}
	ch <- bs
	return
}

func (d *Decoder) Decode(v interface{}) (err error) {
	one, err := d.ReadOne()
	if err != nil {
		return
	}
	return gbson.Unmarshal(one, v)
}

func (d *Decoder) ReadOne() (one BSON, err error) {
	return ReadOne(d.r)
}

func NewEncoder(w io.Writer) *gbson.Encoder {
	return gbson.NewEncoder(w)
}

func ReadOne(r io.Reader) (one []byte, err error) {
	return bson.ReadOne(r)
}

func Marshal(in interface{}) (out []byte, err error) {
	return gbson.Marshal(in)
}

func Unmarshal(in []byte, out interface{}) (err error) {
	return gbson.Unmarshal(in, out)
}
