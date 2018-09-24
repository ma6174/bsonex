package bsonex

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	gbson "github.com/globalsign/mgo/bson"
	"github.com/golib/assert"
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

func TestBsonGet(t *testing.T) {
	now := time.Now()
	ts, _ := gbson.NewMongoTimestamp(time.Now(), 1)
	doc := M{
		"float64":   float64(7.8),                                                   // 0x01
		"string":    "value of str",                                                 // 0x02
		"doc":       M{"int64": int64(321)},                                         // 0x03
		"array":     []int64{22, 33},                                                // 0x04
		"binary":    []byte("binary val"),                                           // 0x05
		"undefined": gbson.Undefined,                                                // 0x06
		"objid":     gbson.NewObjectId(),                                            // 0x07
		"true":      true,                                                           // 0x08
		"false":     false,                                                          // 0x08
		"timestamp": ts,                                                             // 0x09
		"null":      nil,                                                            // 0x0A
		"regex":     gbson.RegEx{Pattern: "pattern[a-z]+", Options: "is"},           // 0x0B
		"DBPointer": gbson.DBPointer{Namespace: "test.rs", Id: gbson.NewObjectId()}, // 0x0C
		// 0x0D JavaScript code
		"int32": int32(456), // 0x10
		"time":  now,        // 0x11
		"int64": int64(123), // 0x12
		// 0x13
		"min": gbson.MinKey,
		"max": gbson.MaxKey,
	}
	b, err := Marshal(doc)
	assert.NoError(t, err)
	bs := BSON(b)
	assert.Equal(t, int64(123), bs.Lookup("int64").Int64())
	assert.Equal(t, int32(456), bs.Lookup("int32").Int32())
	assert.Equal(t, float64(7.8), bs.Lookup("float64").Float64())
	assert.Equal(t, "value of str", bs.Lookup("string").String())
	assert.Equal(t, []byte("binary val"), bs.Lookup("binary").Binary().Data)
	assert.Equal(t, byte(0x0), bs.Lookup("binary").Binary().Type)
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
}
