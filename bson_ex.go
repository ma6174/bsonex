package bsonex

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/sbunce/bson"
	gbson "gopkg.in/mgo.v2/bson"
)

type M map[string]interface{}

type BSON []byte

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
	m, err := b.Map()
	if err != nil {
		panic(err)
	}
	s, err = json.Marshal(m)
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

func (d *Decoder) Do(parallel int, f func(b BSON) error) (err error) {
	var ch = make(chan []byte, parallel*2)
	var errCh = make(chan error, parallel*2)
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

func (d *Decoder) ReadOne() (one []byte, err error) {
	return bson.ReadOne(d.r)
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w}
}

type Encoder struct {
	w io.Writer
}

func (e *Encoder) Encode(v interface{}) (err error) {
	b, err := gbson.Marshal(v)
	if err != nil {
		return
	}
	_, err = e.w.Write(b)
	return
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
