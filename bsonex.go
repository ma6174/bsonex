package bsonex

import (
	"bufio"
	"io"
	"sync"
)

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
	return Unmarshal(one, v)
}

func (d *Decoder) ReadOne() (one []byte, err error) {
	return ReadOne(d.r)
}
