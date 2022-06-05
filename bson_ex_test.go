package bsonex

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	gbson "github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
)

type Doc struct {
	A int
	B float64
	C string
	D []byte
}

func TestBsonEx(t *testing.T) {
	doc := Doc{
		A: 1,
		B: 2.3,
		C: "3.4",
		D: []byte("4.5"),
	}
	b, err := Marshal(doc)
	assert.NoError(t, err)

	b1 := BSON(b)
	j, err := b1.ToJson()
	assert.NoError(t, err)
	var doc2, doc3 Doc

	err = Unmarshal(b, &doc2)
	assert.NoError(t, err)
	assert.Equal(t, doc, doc2)

	err = json.Unmarshal(j, &doc3)
	assert.NoError(t, err)
	assert.Equal(t, doc, doc3)
}

var (
	now   = time.Now()
	ts, _ = gbson.NewMongoTimestamp(time.Now(), 1)
	id    = gbson.NewObjectId()
	doc   = M{
		"float64":   float64(-7.8),                                        // 0x01
		"string":    "value of str",                                       // 0x02
		"doc":       M{"int64": int64(321)},                               // 0x03
		"array":     []int64{22, 33},                                      // 0x04
		"binary":    []byte("binary val"),                                 // 0x05
		"undefined": gbson.Undefined,                                      // 0x06
		"objid":     gbson.NewObjectId(),                                  // 0x07
		"true":      true,                                                 // 0x08
		"false":     false,                                                // 0x08
		"timestamp": ts,                                                   // 0x09
		"null":      nil,                                                  // 0x0A
		"regex":     gbson.RegEx{Pattern: "pattern[a-z]+", Options: "is"}, // 0x0B
		"DBPointer": gbson.DBPointer{Namespace: "test.rs", Id: id},        // 0x0C
		// 0x0D JavaScript code
		"int32": int32(-456), // 0x10
		"time":  now,         // 0x11
		"int64": int64(-123), // 0x12
		// 0x13
		"min": gbson.MinKey,
		"max": gbson.MaxKey,
	}
)

func TestJson(t *testing.T) {
	o, err := Marshal(doc)
	assert.NoError(t, err)
	b := BSON(o)
	fmt.Println(b)
}

func TestValueMap(t *testing.T) {
	o, err := Marshal(doc)
	assert.NoError(t, err)
	b := BSON(o)
	vals := b.Map()
	assert.Equal(t, doc["float64"], vals["float64"].(float64), "float64")
	assert.Equal(t, doc["string"], vals["string"].(string), "string")
	assert.Equal(t, doc["true"], vals["true"].(bool), "bool")
	assert.Equal(t, doc["false"], vals["false"].(bool), "bool")
	assert.Equal(t, doc["int32"], int32(vals["int32"].(uint32)), "int32")
	assert.Equal(t, doc["int64"], int64(vals["int64"].(uint64)), "int64")
}

func TestBsonGet(t *testing.T) {
	b, err := Marshal(doc)
	assert.NoError(t, err)
	bs := BSON(b)
	assert.Equal(t, int64(-123), bs.Lookup("int64").Int64())
	assert.Equal(t, int32(-456), bs.Lookup("int32").Int32())
	assert.Equal(t, int64(-456), bs.Lookup("int32").Int64())
	assert.Equal(t, float64(-7.8), bs.Lookup("float64").Float64())
	assert.Equal(t, "value of str", bs.Lookup("string").Str())
	assert.Equal(t, []byte("binary val"), bs.Lookup("binary").Binary().Data)
	assert.Equal(t, byte(0x0), bs.Lookup("binary").Binary().Kind)
	assert.True(t, bs.Lookup("null").IsNull())
	assert.Equal(t, int64(321), bs.Lookup("doc").Document().Lookup("int64").Int64())
	assert.Equal(t, int64(22), bs.Lookup("array").ArrayOf(0).Int64())
	assert.Equal(t, int64(33), bs.Lookup("array").ArrayOf(1).Int64())
	assert.Equal(t, uint16(os.Getpid()), bs.Lookup("objid").Objid().Pid())
	assert.True(t, bs.Lookup("true").Bool())
	assert.False(t, bs.Lookup("false").Bool())
	assert.Equal(t, now.Nanosecond()/1e6*1e6, bs.Lookup("time").Time().Nanosecond())
	assert.Equal(t, "pattern[a-z]+", bs.Lookup("regex").Regexp().Pattern)
	assert.Equal(t, "is", bs.Lookup("regex").Regexp().Options)
	assert.True(t, bs.Lookup("undefined").IsUndefined())
	assert.True(t, bs.Lookup("min").IsMinKey())
	assert.True(t, bs.Lookup("max").IsMaxKey())
	assert.Equal(t, ts, bs.Lookup("timestamp").MongoTimestamp())
	assert.Equal(t, "test.rs", bs.Lookup("DBPointer").DBPointer().Namespace)
	assert.Equal(t, id, bs.Lookup("DBPointer").DBPointer().Id)
	assert.Equal(t, int64(321), bs.Lookup("doc.int64").Int64(), "Lookup many")
	assert.Equal(t, int64(0), bs.Lookup("doc.x").Int64(), "Lookup many")
	assert.Equal(t, int64(0), bs.Lookup("doc.x.x").Int64(), "Lookup many")
	assert.Equal(t, int64(0), bs.Lookup(".").Int64(), "Lookup many")
}

func TestDo(t *testing.T) {
	{
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			w := NewEncoder(pw)
			for i := 0; i < 4; i++ {
				err := w.Encode(M{"i": i})
				assert.NoError(t, err)
			}
		}()
		var sum int
		NewDecoder(pr).Do(1, func(b BSONEX) (err error) {
			sum += int(b.Map()["i"].(uint32))
			return
		})
		assert.Equal(t, 6, sum)
	}
	{
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			w := NewEncoder(pw)
			for i := 1; i <= 100; i++ {
				err := w.Encode(M{"i": i})
				assert.NoError(t, err)
			}
		}()
		var sum int
		NewDecoder(pr).Do(10, func(b BSONEX) (err error) {
			sum += int(b.Map()["i"].(uint32))
			return
		})
		assert.Equal(t, 5050, sum)
	}
}

func TestContains(t *testing.T) {
	containsCases := map[interface{}]interface{}{
		"abc": gbson.M{"abc": "def"},
		"a":   gbson.M{"abc": "def"},
		"def": gbson.M{"abc": "def"},
		"d":   gbson.M{"abc": "def"},
		123:   gbson.M{"abc": 123},
		3.14:  gbson.M{"abc": 3.14},
	}
	for k, v := range containsCases {
		bs, err := gbson.Marshal(v)
		assert.NoError(t, err)
		toSearchValue, err := NewToSearchValue(k)
		assert.NoError(t, err)
		assert.True(t, BSON(bs).FastContains(toSearchValue), k, v)
	}
	// search doc
	{
		doc := gbson.M{"abc": "sdkf"}
		bs, err := gbson.Marshal(gbson.M{"k": doc})
		assert.NoError(t, err)
		toSearchValue, err := NewToSearchValue(doc)
		assert.NoError(t, err)
		assert.True(t, BSON(bs).FastContains(toSearchValue), doc)
	}
}

func BenchmarkUnmarshalStruct(b *testing.B) {
	bs, err := Marshal(doc)
	assert.NoError(b, err)
	type Doc struct {
		Float64 float64 `bson:"float64"`
		Int64   int64   `bson:"int64"`
		String  string  `bson:"string"`
	}
	var doc Doc
	for i := 0; i < b.N; i++ {
		_ = Unmarshal(bs, &doc)
	}
}

func BenchmarkLookup(b *testing.B) {
	bs, err := Marshal(doc)
	assert.NoError(b, err)
	bsb := BSON(bs)
	type Doc struct {
		Float64 float64 `bson:"float64"`
		Int64   int64   `bson:"int64"`
		String  string  `bson:"string"`
	}
	var doc Doc
	for i := 0; i < b.N; i++ {
		doc.Float64 = bsb.Lookup("float64").Float64()
		doc.Int64 = bsb.Lookup("int64").Int64()
		doc.String = bsb.Lookup("string").Str()
	}
}

func BenchmarkUnmarshalMap(b *testing.B) {
	bs, err := Marshal(doc)
	assert.NoError(b, err)
	var doc M
	for i := 0; i < b.N; i++ {
		_ = Unmarshal(bs, &doc)
	}
}

func BenchmarkToMap(b *testing.B) {
	bs, err := Marshal(doc)
	assert.NoError(b, err)
	bsb := BSON(bs)
	for i := 0; i < b.N; i++ {
		_ = bsb.Map()
	}
}

func BenchmarkToValueMap(b *testing.B) {
	bs, err := Marshal(doc)
	assert.NoError(b, err)
	bsb := BSON(bs)
	for i := 0; i < b.N; i++ {
		_ = bsb.ToValueMap()
	}
}

func TestIsEmpty(t *testing.T) {
	bs, err := Marshal(doc)
	assert.NoError(t, err)
	bsb := BSON(bs)
	assert.True(t, bsb.Lookup("x").IsEmpty())
	assert.False(t, bsb.Lookup("doc").IsEmpty())
	assert.False(t, bsb.Lookup("doc.int64").IsEmpty())
	assert.True(t, bsb.Lookup("doc.x").IsEmpty())
}
