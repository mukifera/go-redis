package resp

import (
	"strconv"
	"sync"
)

type Object interface {
	Encode() []byte
}

type SimpleString string
type SimpleError string
type Integer int
type BulkString string
type NullBulkString struct{}
type Array []Object
type Set map[Object]struct{}
type Map map[Object]Object
type Boolean bool
type Stream struct {
	Mu      sync.Mutex
	Entries []struct {
		Id   string
		Data map[string]Object
	}
}

func (r SimpleString) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '+')
	ret = append(ret, r...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r SimpleError) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '-')
	ret = append(ret, r...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r Integer) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, ':')
	if r < 0 {
		ret = append(ret, '-')
		r = -r
	}
	ret = append(ret, strconv.Itoa(int(r))...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r BulkString) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '$')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	ret = append(ret, r...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r NullBulkString) Encode() []byte {
	return []byte("$-1\r\n")
}

func (r Array) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '*')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	for i := 0; i < len(r); i++ {
		ret = append(ret, r[i].Encode()...)
	}
	return ret
}

func (r Set) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '~')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	for key := range r {
		ret = append(ret, key.Encode()...)
	}
	return ret
}

func (r Boolean) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '#')
	if r {
		ret = append(ret, 't')
	} else {
		ret = append(ret, 'f')
	}
	ret = append(ret, "\r\n"...)
	return ret
}

func (r Map) Encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '%')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	for key, value := range r {
		ret = append(ret, key.Encode()...)
		ret = append(ret, value.Encode()...)
	}
	return ret
}

func (r Stream) Encode() []byte {
	return nil
}

func (r *Stream) AddEntry(id string, data map[string]Object) {
	r.Entries = append(r.Entries, struct {
		Id   string
		Data map[string]Object
	}{
		Id:   id,
		Data: data,
	})
}

func StringsToArray(arr []string) Array {
	var ret Array = make([]Object, len(arr))
	for i := 0; i < len(arr); i++ {
		ret[i] = BulkString(arr[i])
	}
	return ret
}

func ToString(obj Object) (string, bool) {
	simple, ok := obj.(SimpleString)
	if ok {
		return string(simple), true
	}
	bulk, ok := obj.(BulkString)
	if ok {
		return string(bulk), true
	}
	return "", false
}

func ToInt(obj Object) (int, bool) {
	integer, ok := obj.(Integer)
	if ok {
		return int(integer), true
	}
	str, ok := ToString(obj)
	if ok {
		ret, err := strconv.Atoi(str)
		if err == nil {
			return ret, true
		}
	}
	return 0, false
}

func Decode(in <-chan byte) (n int, ret Object) {

	ch := <-in
	switch ch {
	case '+':
		n, ret = decodeSimpleString(in)
	case '-':
		var str SimpleString
		n, str = decodeSimpleString(in)
		ret = SimpleError(str)
	case ':':
		n, ret = DecodeInteger(in)
	case '$':
		n, ret = decodeBulkString(in)
	case '*':
		n, ret = decodeArray(in)
	case '_':
		<-in
		<-in
		n = 2
		ret = nil
	case '#':
		n, ret = decodeBoolean(in)
	// case ',':
	// 	return decodeDouble(in)
	// case '(':
	// 	return decodeBigNumber(in)
	// case '!':
	// 	return decodeBulkError(in)
	// case '=':
	// 	return decodeVerbatimString(in)
	case '%':
		n, ret = decodeMap(in)
	// case '|':
	// 	return decodeAttribute(in)
	case '~':
		n, ret = decodeSet(in)
		// case '>':
		// 	return decodePush(in)

	default:
		return 1, nil
	}

	return n + 1, ret
}

func decodeSimpleString(in <-chan byte) (int, SimpleString) {
	n := 0
	buf := make([]byte, 0)
	for {
		if len(buf) > 1 && buf[len(buf)-2] == '\r' && buf[len(buf)-1] == '\n' {
			break
		}
		buf = append(buf, <-in)
		n++
	}
	return n, SimpleString(string(buf[:len(buf)-2]))
}

func DecodeInteger(in <-chan byte) (int, Integer) {
	negative := false
	value := 0
	n := 0

	ch := <-in
	if ch == '-' {
		negative = true
	} else if ch != '+' {
		value = int(ch - '0')
	}
	n++

	for {
		ch = <-in
		n++
		if ch == '\r' {
			break
		}
		value *= 10
		value += int(ch - '0')
	}

	<-in
	n++

	if negative {
		value = -value
	}
	return n, Integer(value)
}

func decodeBulkString(in <-chan byte) (int, BulkString) {
	n, length := DecodeInteger(in)

	if length == -1 {
		return n, ""
	}

	nn, str := decodeSimpleString(in)
	n += nn

	return n, BulkString(str)
}

func decodeArray(in <-chan byte) (int, Array) {
	n, length := DecodeInteger(in)

	if length == -1 {
		return n, nil
	}

	arr := make([]Object, length)
	nn := 0
	for i := 0; i < int(length); i++ {
		nn, arr[i] = Decode(in)
		n += nn
	}
	return n, arr
}

func decodeBoolean(in <-chan byte) (int, Boolean) {
	ch := <-in
	<-in
	<-in
	return 3, ch == 't'
}

func decodeMap(in <-chan byte) (int, Map) {
	dict := make(map[Object]Object)
	n, length := DecodeInteger(in)
	for i := 0; i < int(length); i++ {
		nn, key := Decode(in)
		n += nn
		nn, value := Decode(in)
		n += nn
		dict[key] = value
	}
	return n, dict
}

func decodeSet(in <-chan byte) (int, Set) {
	dict := make(map[Object]struct{})
	n, length := DecodeInteger(in)
	for i := 0; i < int(length); i++ {
		nn, value := Decode(in)
		n += nn
		dict[value] = struct{}{}
	}
	return n, dict
}
