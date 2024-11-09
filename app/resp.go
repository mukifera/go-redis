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

func decode(in <-chan byte) respObject {

	ch := <-in
	switch ch {
	case '+':
		return decodeSimpleString(in)
	case '-':
		return respSimpleError(decodeSimpleString(in))
	case ':':
		return decodeInteger(in)
	case '$':
		return decodeBulkString(in)
	case '*':
		return decodeArray(in)
	case '_':
		<-in
		<-in
		return nil
	case '#':
		return decodeBoolean(in)
	// case ',':
	// 	return decodeDouble(in)
	// case '(':
	// 	return decodeBigNumber(in)
	// case '!':
	// 	return decodeBulkError(in)
	// case '=':
	// 	return decodeVerbatimString(in)
	case '%':
		return decodeMap(in)
	// case '|':
	// 	return decodeAttribute(in)
	case '~':
		return decodeSet(in)
		// case '>':
		// 	return decodePush(in)

	}
	return nil
}

func decodeSimpleString(in <-chan byte) respSimpleString {
	buf := make([]byte, 0)
	for {
		if len(buf) > 1 && buf[len(buf)-2] == '\r' && buf[len(buf)-1] == '\n' {
			break
		}
		buf = append(buf, <-in)
	}
	return respSimpleString(string(buf[:len(buf)-2]))
}

func decodeInteger(in <-chan byte) respInteger {
	negative := false
	value := 0

	ch := <-in
	if ch == '-' {
		negative = true
	} else if ch != '+' {
		value = int(ch - '0')
	}

	for {
		ch = <-in
		if ch == '\r' {
			break
		}
		value *= 10
		value += int(ch - '0')
	}

	<-in

	if negative {
		value = -value
	}
	return respInteger(value)
}

func decodeBulkString(in <-chan byte) respBulkString {
	length := decodeInteger(in)

	if length == -1 {
		return ""
	}

	return respBulkString(decodeSimpleString(in))
}

func decodeArray(in <-chan byte) respArray {
	length := decodeInteger(in)

	if length == -1 {
		return nil
	}

	arr := make([]respObject, length)
	for i := 0; i < int(length); i++ {
		arr[i] = decode(in)
	}
	return arr
}

func decodeBoolean(in <-chan byte) respBoolean {
	ch := <-in
	<-in
	<-in
	return ch == 't'
}

func decodeMap(in <-chan byte) respMap {
	dict := make(map[respObject]respObject)
	length := decodeInteger(in)
	for i := 0; i < int(length); i++ {
		key := decode(in)
		value := decode(in)
		dict[key] = value
	}
	return dict
}

func decodeSet(in <-chan byte) respSet {
	dict := make(map[respObject]struct{})
	length := decodeInteger(in)
	for i := 0; i < int(length); i++ {
		value := decode(in)
		dict[value] = struct{}{}
	}
	return dict
}
