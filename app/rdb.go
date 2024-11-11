package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"os"
)

type contentType byte

const (
	NORMAL contentType = iota
	UINT8
	UINT16
	UINT32
	UNSUPPORTED
)

var opCodes = struct {
	EOF          byte
	SELECTDB     byte
	EXPIRETIME   byte
	EXPIRETIMEMS byte
	RESIZEDB     byte
	AUX          byte
}{
	EOF:          0xFF,
	SELECTDB:     0xFE,
	EXPIRETIME:   0xFD,
	EXPIRETIMEMS: 0xFC,
	RESIZEDB:     0xFB,
	AUX:          0xFA,
}

var rdbValueTypes = struct {
	STRING             byte
	LIST               byte
	SET                byte
	SORTED_SET         byte
	HASH               byte
	ZIPMAP             byte
	ZIPLIST            byte
	INTSET             byte
	SORTED_SET_ZIPLIST byte
	HASHMAP_ZIPLIST    byte
	LIST_QUICKLIST     byte
}{
	STRING:             0,
	LIST:               1,
	SET:                2,
	SORTED_SET:         3,
	HASH:               4,
	ZIPMAP:             9,
	ZIPLIST:            10,
	INTSET:             11,
	SORTED_SET_ZIPLIST: 12,
	HASHMAP_ZIPLIST:    13,
	LIST_QUICKLIST:     14,
}

func readRDBFile(filename string) (*redisStore, error) {
	var store redisStore
	var current uint64 = 9
	store.init()

	data, err := os.ReadFile(filename)
	if errors.Is(err, os.ErrNotExist) {
		return &store, nil
	}
	if err != nil {
		return &store, fmt.Errorf("unable to read RDB file: %w", err)
	}

	header := string(data[:9])
	if header[:5] != "REDIS" {
		return &store, fmt.Errorf("unknown header in RDB file")
	}

	for {
		section := data[current]
		current++
		switch section {
		case opCodes.AUX:
			n, key := readEncodedString(data[current:])
			current += n
			n, value := readEncodedString(data[current:])
			current += n
			store.setParam(key, value)

		case opCodes.RESIZEDB:
			n, kv_size := readLengthEncodedInt(data[current:])
			current += uint64(n)
			n, expiry_size := readLengthEncodedInt(data[current:])
			current += uint64(n)
			fmt.Printf("keyspace size: %d, expiry space size: %d\n", kv_size, expiry_size)

		case opCodes.EXPIRETIMEMS:
			expiry := binary.LittleEndian.Uint64(data[current:])
			current += 8
			n, key, value := readKeyValue(data[current:])
			current += n
			store.setWithAbsoluteExpiry(key, value, expiry)

		case opCodes.EXPIRETIME:
			expiry := uint64(binary.LittleEndian.Uint32(data[current:]))
			current += 4
			n, key, value := readKeyValue(data[current:])
			current += n
			store.setWithAbsoluteExpiry(key, value, expiry*1000)

		case opCodes.SELECTDB:
			n, size, content_type := readEncodedSize(data[current:])
			current += uint64(n)
			fmt.Printf("size: %d, content_type: %d\n", size, content_type)

		case opCodes.EOF:
			checksum := data[current : current+8]
			fmt.Println(checksum)
			return &store, nil

		case rdbValueTypes.LIST, rdbValueTypes.SET, rdbValueTypes.STRING:
			current -= 1
			n, key, value := readKeyValue(data[current:])
			current += n
			store.set(key, value)

		default:
			return &store, errors.New("malformed RDB file")
		}
	}
}

func readEncodedString(data []byte) (bytes_read uint64, str string) {
	bytes_read = 0
	str = ""

	n, size, content_type := readEncodedSize(data)
	bytes_read += uint64(n)
	switch content_type {
	case NORMAL:
		str = string(data[bytes_read : bytes_read+uint64(size)])
	case UINT8, UINT16, UINT32:
		_, integer := readLengthEncodedInt(data)
		str = fmt.Sprintf("%d", integer)
	default:
		fmt.Fprintf(os.Stderr, "unsupported string type")
		os.Exit(1)
	}
	bytes_read += uint64(size)

	return
}

func readEncodedSize(data []byte) (bytes_read uint8, size uint32, content_type contentType) {
	bytes_read = 0
	size = 0
	content_type = NORMAL

	bits := (0b11000000 & data[0]) >> 6
	switch bits {
	case 0b00:
		bytes_read = 1
		size = 0b00111111 & uint32(data[0])
		content_type = NORMAL
	case 0b01:
		bytes_read = 2
		size = (0b00111111 & uint32(data[0])) << 8
		size += uint32(data[1])
		content_type = NORMAL
	case 0b10:
		bytes_read = 5
		size = binary.BigEndian.Uint32(data[1:])
		content_type = NORMAL
	case 0b11:
		bits := 0b00111111 & data[0]
		switch bits {
		case 0:
			bytes_read = 1
			size = 1
			content_type = contentType(UINT8)
		case 1:
			bytes_read = 1
			size = 2
			content_type = UINT16
		case 2:
			bytes_read = 1
			size = 4
			content_type = UINT32
		default:
			bytes_read = 0
			size = 0
			content_type = UNSUPPORTED
		}
	}
	return
}

func readLengthEncodedInt(data []byte) (bytes_read uint8, integer uint32) {
	bytes_read = 0
	integer = 0

	bytes_read, size, content_type := readEncodedSize(data)

	switch content_type {
	case NORMAL:
		integer = size
		return
	case UINT8:
		integer = uint32(data[bytes_read])
	case UINT16:
		integer = uint32(binary.LittleEndian.Uint16(data[bytes_read : bytes_read+uint8(size)]))
	case UINT32:
		integer = binary.LittleEndian.Uint32(data[bytes_read : bytes_read+uint8(size)])
	default:
		fmt.Fprintf(os.Stderr, "unsupported size encoding")
		os.Exit(1)
	}

	bytes_read += uint8(size)

	return
}

func readKeyValue(data []byte) (bytes_read uint64, key string, value respObject) {

	data_type := data[0]

	bytes_read = 1

	n, key := readEncodedString(data[bytes_read:])
	bytes_read += n

	switch data_type {
	case rdbValueTypes.STRING:
		n, str := readEncodedString(data[bytes_read:])
		value = respBulkString(str)
		bytes_read += n
	case rdbValueTypes.LIST:
		n, list := readRDBList(data[bytes_read:])
		value = stringArrayToResp(list)
		bytes_read += n
	case rdbValueTypes.SET:
		n, set := readRDBSet(data[bytes_read:])
		value = respSet(set)
		bytes_read += n
	default:
		fmt.Fprintf(os.Stderr, "unsupported key/value type\n")
		os.Exit(1)
	}

	return
}

func readRDBList(data []byte) (bytes_read uint64, list []string) {
	bytes_read = 0

	n, size := readLengthEncodedInt(data)
	bytes_read += uint64(n)
	list = make([]string, size)
	var i uint32
	for i = 0; i < size; i++ {
		n, str := readEncodedString(data[bytes_read:])
		bytes_read += n
		list[i] = str
	}

	return
}

func readRDBSet(data []byte) (bytes_read uint64, set map[respObject]struct{}) {
	bytes_read, list := readRDBList(data)
	set = make(map[respObject]struct{})
	for _, str := range list {
		set[respBulkString(str)] = struct{}{}
	}
	return
}

func generateRDBFile(_ *redisStore) []byte {
	data := make([]byte, 0)
	data = append(data, "REDIS0011"...)
	data = append(data, 0xFE, 00)
	data = append(data, 0xFB, 0x00, 0x00)

	var poly uint64 = 0xad93d23594c935a9
	table := crc64.MakeTable(poly)
	checksum := crc64.New(table).Sum(data)
	data = append(data, 0xFF)
	data = append(data, checksum...)
	return data
}