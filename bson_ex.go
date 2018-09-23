package bsonex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	gbson "github.com/globalsign/mgo/bson"
	"github.com/sbunce/bson"
)

type M map[string]interface{}

type BSON []byte

func getElement(b BSON) (key string, val Value, next BSON) {
	if len(b) == 0 {
		return "", val, nil
	}
	elementType := b[0]
	keyStart, keyEnd := 1, bytes.IndexByte(b, 0x00)
	key = string(b[keyStart:keyEnd])
	var valb []byte
	switch elementType {
	case 0x1, 0x09, 0x11, 0x12: // double,UTC datetime,Timestamp, 64-bit integer
		valb = b[keyEnd+1 : keyEnd+1+8]
	case 0x2, 0x0D, 0x0E, 0x0F: // string, JavaScript code, Symbol,JavaScript code w/ scope
		strLen := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+4+strLen]
	case 0x3, 0x4: // Embedded document, Array
		Len := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+Len]
	case 0x5: // binary
		Len := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+4+1+Len]
	case 0x06, 0x0A, 0xFF, 0x7F: // Undefined, null value, 	Min key, Max key
	case 0x07: // 	ObjectId
		valb = b[keyEnd+1 : keyEnd+1+12]
	case 0x08: // bool
		valb = b[keyEnd+1 : keyEnd+1+1]
	case 0x0B: // Regular expression
		i := bytes.IndexByte(b[keyEnd+1:], 0x00)
		i2 := bytes.IndexByte(b[keyEnd+1+i+1:], 0x00)
		valb = b[keyEnd+1 : keyEnd+1+i+1+i2+1]
	case 0x0C: // DBPointer
		strLen := getint(b[keyEnd+1 : keyEnd+1+4])
		valb = b[keyEnd+1 : keyEnd+1+4+int(strLen)+12]
	case 0x10: // 32-bit integer
		valb = b[keyEnd+1 : keyEnd+1+4]
	case 0x13: // 128-bit decimal floating point
		valb = b[keyEnd+1 : keyEnd+1+16]
	default:
		panic(fmt.Sprintf("invalid bson type %#v", elementType))
	}
	val, next = Value{elementType, valb}, b[len(key)+1+len(valb)+1:]
	return
}

// return nil if not found
func (b BSON) Lookup(key string) (val Value) {
	defer func() {
		e := recover()
		if e == nil {
			return
		}
		log.Println(key, string(b.MustToJson()))
		panic(e)
	}()
	elements := b[4 : len(b)-1]
	for elements != nil {
		ckey, cval, next := getElement(elements)
		if ckey == key {
			return cval
		}
		elements = next
	}
	return
}

func (b BSON) Unmarshal(out interface{}) (err error) {
	return gbson.Unmarshal(b, out)
}

func (b BSON) Map() (m M, err error) {
	err = gbson.Unmarshal(b, &m)
	return
}

func (b BSON) ToJson() (s []byte, err error) {
	m, err := b.Map()
	if err != nil {
		return
	}
	return json.Marshal(m)
}
func (b BSON) MustToJson() (s []byte) {
	s, err := b.ToJson()
	if err != nil {
		panic(err)
	}
	return
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r}
}

type Decoder struct {
	r io.Reader
}

func (d *Decoder) ForEach(f func(b BSON) error) (err error) {
	for {
		one, err := d.ReadOne()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = f(one)
		if err != nil {
			return err
		}
	}
	return
}

func (d *Decoder) Do(parallel int, f func(b BSON) error) (err error) {
	var ch = make(chan []byte, parallel*1000)
	var errCh = make(chan error, parallel+1)
	var wg sync.WaitGroup
	var once sync.Once
	wg.Add(parallel)
	defer once.Do(func() { close(ch) })
	for i := 0; i < parallel; i++ {
		go func() {
			defer wg.Done()
			for b := range ch {
				err := f(b)
				if err != nil {
					errCh <- err
					break
				}
			}
		}()
	}
	go func() {
		for {
			one, err := d.ReadOne()
			if err != nil {
				if err == io.EOF {
					break
				}
				errCh <- err
				break
			}
			ch <- one
		}
		once.Do(func() { close(ch) })
	}()
	go func() {
		wg.Wait()
		close(errCh)
	}()
	return <-errCh
}

func (d *Decoder) Decode(v interface{}) (err error) {
	one, err := d.ReadOne()
	if err != nil {
		return
	}
	return gbson.Unmarshal(one, v)
}

func (d *Decoder) ReadOne() (one BSON, err error) {
	b, err := bson.ReadOne(d.r)
	return BSON(b), err
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
