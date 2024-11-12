package main

import (
	"strconv"
)

type respObject interface {
	encode() []byte
}

type respSimpleString string
type respSimpleError string
type respInteger int
type respBulkString string
type respNullBulkString struct{}
type respArray []respObject
type respSet map[respObject]struct{}
type respMap map[respObject]respObject
type respBoolean bool

func (r respSimpleString) encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '+')
	ret = append(ret, r...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r respSimpleError) encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '-')
	ret = append(ret, r...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r respInteger) encode() []byte {
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

func (r respBulkString) encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '$')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	ret = append(ret, r...)
	ret = append(ret, "\r\n"...)
	return ret
}

func (r respNullBulkString) encode() []byte {
	return []byte("$-1\r\n")
}

func (r respArray) encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '*')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	for i := 0; i < len(r); i++ {
		ret = append(ret, r[i].encode()...)
	}
	return ret
}

func (r respSet) encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '~')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	for key := range r {
		ret = append(ret, key.encode()...)
	}
	return ret
}

func (r respBoolean) encode() []byte {
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

func (r respMap) encode() []byte {
	ret := make([]byte, 0)
	ret = append(ret, '%')
	ret = append(ret, strconv.Itoa(len(r))...)
	ret = append(ret, "\r\n"...)
	for key, value := range r {
		ret = append(ret, key.encode()...)
		ret = append(ret, value.encode()...)
	}
	return ret
}

func stringArrayToResp(arr []string) respArray {
	var ret respArray = make([]respObject, len(arr))
	for i := 0; i < len(arr); i++ {
		ret[i] = respBulkString(arr[i])
	}
	return ret
}

func respToString(obj respObject) (string, bool) {
	simple, ok := obj.(respSimpleString)
	if ok {
		return string(simple), true
	}
	bulk, ok := obj.(respBulkString)
	if ok {
		return string(bulk), true
	}
	return "", false
}

func respToInt(obj respObject) (int, bool) {
	integer, ok := obj.(respInteger)
	if ok {
		return int(integer), true
	}
	str, ok := respToString(obj)
	if ok {
		ret, err := strconv.Atoi(str)
		if err == nil {
			return ret, true
		}
	}
	return 0, false
}

func decode(in <-chan byte) (n int, ret respObject) {

	ch := <-in
	switch ch {
	case '+':
		n, ret = decodeSimpleString(in)
	case '-':
		var str respSimpleString
		n, str = decodeSimpleString(in)
		ret = respSimpleError(str)
	case ':':
		n, ret = decodeInteger(in)
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
		return 0, nil
	}

	return n + 1, ret
}

func decodeSimpleString(in <-chan byte) (int, respSimpleString) {
	n := 0
	buf := make([]byte, 0)
	for {
		if len(buf) > 1 && buf[len(buf)-2] == '\r' && buf[len(buf)-1] == '\n' {
			break
		}
		buf = append(buf, <-in)
		n++
	}
	return n, respSimpleString(string(buf[:len(buf)-2]))
}

func decodeInteger(in <-chan byte) (int, respInteger) {
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
	return n, respInteger(value)
}

func decodeBulkString(in <-chan byte) (int, respBulkString) {
	n, length := decodeInteger(in)

	if length == -1 {
		return n, ""
	}

	nn, str := decodeSimpleString(in)
	n += nn

	return n, respBulkString(str)
}

func decodeArray(in <-chan byte) (int, respArray) {
	n, length := decodeInteger(in)

	if length == -1 {
		return n, nil
	}

	arr := make([]respObject, length)
	nn := 0
	for i := 0; i < int(length); i++ {
		nn, arr[i] = decode(in)
		n += nn
	}
	return n, arr
}

func decodeBoolean(in <-chan byte) (int, respBoolean) {
	ch := <-in
	<-in
	<-in
	return 3, ch == 't'
}

func decodeMap(in <-chan byte) (int, respMap) {
	dict := make(map[respObject]respObject)
	n, length := decodeInteger(in)
	for i := 0; i < int(length); i++ {
		nn, key := decode(in)
		n += nn
		nn, value := decode(in)
		n += nn
		dict[key] = value
	}
	return n, dict
}

func decodeSet(in <-chan byte) (int, respSet) {
	dict := make(map[respObject]struct{})
	n, length := decodeInteger(in)
	for i := 0; i < int(length); i++ {
		nn, value := decode(in)
		n += nn
		dict[value] = struct{}{}
	}
	return n, dict
}
