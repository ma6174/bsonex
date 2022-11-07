package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/globalsign/mgo/bson"
	"github.com/ma6174/bsonex"
)

type parsers map[string]func(input string) interface{}

func (p parsers) All() (all []string) {
	for t := range p {
		all = append(all, t)
	}
	return all
}

func (p parsers) Parse(t, input string) interface{} {
	if parser, ok := p[t]; ok {
		return parser(input)
	}
	panic("unknown type, avaliable type: " + strings.Join(p.All(), ","))
}

var int64Parser = func(input string) interface{} {
	i, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		log.Panicln(err)
	}
	return i
}

var float64Parser = func(input string) interface{} {
	i, err := strconv.ParseFloat(input, 64)
	if err != nil {
		log.Panicln(err)
	}
	return i
}

var ps = parsers{
	"string":  func(input string) interface{} { return input },
	"int32":   func(input string) interface{} { return int32(int64Parser(input).(int64)) },
	"int64":   int64Parser,
	"float64": float64Parser,
	"objid":   func(input string) interface{} { return bson.ObjectIdHex(input) },
}

var (
	valueType = flag.String("t", "string", "value type: "+strings.Join(ps.All(), ","))
	key       = flag.String("k", "", "key")
	process   = flag.Int("p", 1, "process")
)

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Printf("usage:\ncat <xxx.bson> | %v -t <type> <to_search_value>\n", os.Args[0])
		return
	}
	v := ps.Parse(*valueType, flag.Arg(0))
	b1, err := bsonex.NewToSearchValue(v)
	if err != nil {
		log.Panicln(err)
	}
	err = bsonex.NewDecoder(os.Stdin).Do(*process, func(b bsonex.BSONEX) (err error) {
		if !b.FastContains(b1) {
			return
		}
		if *key != "" {
			if reflect.DeepEqual(b.Lookup(*key).Value(), v) {
				_, err = os.Stdout.Write(append(b.MustToJson(), '\n'))
			}
			return
		}
		_, err = os.Stdout.Write(append(b.MustToJson(), '\n'))
		return
	})
	if err != nil {
		log.Panicln(err)
	}
}
