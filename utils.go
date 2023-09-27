package bsonex

import "encoding/binary"

func getint(bs []byte) int {
	return int(binary.LittleEndian.Uint32(bs))
}
