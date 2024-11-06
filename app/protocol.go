package main

import (
	"errors"
	"strconv"
)

func decode(in <-chan byte) interface{} {

	ch := <-in
	switch ch {
	case '+':
		return decodeSimpleString(in)
	case '-':
		return errors.New(decodeSimpleString(in))
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

func decodeSimpleString(in <-chan byte) string {
	buf := make([]byte, 0)
	for {
		if len(buf) > 1 && buf[len(buf)-2] == '\r' && buf[len(buf)-1] == '\n' {
			break
		}
		buf = append(buf, <-in)
	}
	return string(buf[:len(buf)-2])
}

func decodeInteger(in <-chan byte) int {
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
	return value
}

func decodeBulkString(in <-chan byte) string {
	length := decodeInteger(in)

	if length == -1 {
		return ""
	}

	return decodeSimpleString(in)
}

func decodeArray(in <-chan byte) []interface{} {
	length := decodeInteger(in)

	if length == -1 {
		return nil
	}

	arr := make([]interface{}, length)
	for i := 0; i < length; i++ {
		arr[i] = decode(in)
	}
	return arr
}

func decodeBoolean(in <-chan byte) bool {
	ch := <-in
	<-in
	<-in
	return ch == 't'
}

func decodeMap(in <-chan byte) map[interface{}]interface{} {
	dict := make(map[interface{}]interface{})
	length := decodeInteger(in)
	for i := 0; i < length; i++ {
		key := decode(in)
		value := decode(in)
		dict[key] = value
	}
	return dict
}

func decodeSet(in <-chan byte) map[interface{}]struct{} {
	dict := make(map[interface{}]struct{})
	length := decodeInteger(in)
	for i := 0; i < length; i++ {
		value := decode(in)
		dict[value] = struct{}{}
	}
	return dict
}

func encode(value interface{}) []byte {
	if value == nil {
		return encodeBulkString(nil)
	}
	switch t := value.(type) {
	case int:
		return encodeInteger(t)
	case string:
		return encodeSimpleString(t)
	case *string:
		return encodeBulkString(t)
	case []interface{}:
		return encodeArray(t)
	}
	return []byte{}
}

func encodeSimpleString(str string) []byte {
	ret := make([]byte, 0)
	ret = append(ret, '+')
	ret = append(ret, str...)
	ret = append(ret, "\r\n"...)
	return ret
}

func encodeBulkString(str *string) []byte {
	if str == nil {
		return []byte("$-1\r\n")
	}
	ret := make([]byte, 0)
	ret = append(ret, '$')
	ret = append(ret, strconv.Itoa(len(*str))...)
	ret = append(ret, "\r\n"...)
	ret = append(ret, *str...)
	ret = append(ret, "\r\n"...)
	return ret
}

func encodeInteger(num int) []byte {
	ret := make([]byte, 0)
	ret = append(ret, ':')
	if num < 0 {
		ret = append(ret, '-')
		num = -num
	}
	ret = append(ret, strconv.Itoa(num)...)
	ret = append(ret, "\r\n"...)
	return ret
}

func encodeArray(arr []interface{}) []byte {
	ret := make([]byte, 0)
	ret = append(ret, '*')
	ret = append(ret, strconv.Itoa(len(arr))...)
	ret = append(ret, "\r\n"...)
	for i := 0; i < len(arr); i++ {
		ret = append(ret, encode(arr[i])...)
	}
	return ret
}
