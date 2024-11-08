package main

import (
	"testing"
)

func TestReadEncodedSize(t *testing.T) {

	tests := []struct {
		name         string
		bytes        []byte
		bytes_read   uint8
		size         uint32
		content_type contentType
	}{
		{name: "0b00", bytes: []byte{0x0A}, bytes_read: 1, size: 10, content_type: NORMAL},
		{name: "0b01", bytes: []byte{0x42, 0xBC}, bytes_read: 2, size: 700, content_type: NORMAL},
		{name: "0b10", bytes: []byte{0x80, 0x00, 0x00, 0x42, 0x68}, bytes_read: 5, size: 17000, content_type: NORMAL},
		{name: "0b11 uint8", bytes: []byte{0xC0}, bytes_read: 1, size: 1, content_type: UINT8},
		{name: "0b11 uint16", bytes: []byte{0xC1}, bytes_read: 1, size: 2, content_type: UINT16},
		{name: "0b11 uint32", bytes: []byte{0xC2}, bytes_read: 1, size: 4, content_type: UINT32},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes_read, size, content_type := readEncodedSize(test.bytes)
			if bytes_read != test.bytes_read || size != test.size || content_type != test.content_type {
				t.Errorf("Expected: bytes_read = %d, size = %d, content_type: %d\nGot: bytes_read: %d, size = %d, content_type: %d",
					test.bytes_read, test.size, test.content_type, bytes_read, size, content_type,
				)
			}
		})
	}
}

func TestReadLengthEncodedInt(t *testing.T) {

	tests := []struct {
		name       string
		bytes      []byte
		bytes_read uint8
		integer    uint32
	}{
		{name: "normal type #1", bytes: []byte{0x03}, bytes_read: 1, integer: 3},
		{name: "normal type #2", bytes: []byte{0x02}, bytes_read: 1, integer: 2},
		{name: "uint8", bytes: []byte{0xC0, 0x7B}, bytes_read: 2, integer: 123},
		{name: "uint16", bytes: []byte{0xC1, 0x39, 0x30}, bytes_read: 3, integer: 12345},
		{name: "uint32", bytes: []byte{0xC2, 0x87, 0xD6, 0x12, 0x00}, bytes_read: 5, integer: 1234567},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes_read, integer := readLengthEncodedInt(test.bytes)
			if bytes_read != test.bytes_read || integer != test.integer {
				t.Errorf("Expected: bytes_read = %d, integer: %d\nGot: bytes_read: %d, integer: %d",
					test.bytes_read, test.integer, bytes_read, integer,
				)
			}
		})
	}
}

func TestReadEncodedString(t *testing.T) {

	tests := []struct {
		name       string
		bytes      []byte
		bytes_read uint64
		str        string
	}{
		{name: "string #1", bytes: []byte{0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72}, bytes_read: 10, str: "redis-ver"},
		{name: "string #2", bytes: []byte{0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36}, bytes_read: 7, str: "6.0.16"},
		{name: "string #3", bytes: []byte{0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72}, bytes_read: 7, str: "foobar"},
		{name: "string #4", bytes: []byte{0x06, 0x62, 0x61, 0x7A, 0x71, 0x75, 0x78}, bytes_read: 7, str: "bazqux"},
		{name: "string #5", bytes: []byte{0x03, 0x66, 0x6F, 0x6F}, bytes_read: 4, str: "foo"},
		{name: "string #6", bytes: []byte{0x03, 0x62, 0x61, 0x72}, bytes_read: 4, str: "bar"},
		{name: "string #7", bytes: []byte{0x03, 0x62, 0x61, 0x7A}, bytes_read: 4, str: "baz"},
		{name: "string #8", bytes: []byte{0x03, 0x71, 0x75, 0x78}, bytes_read: 4, str: "qux"},
		{name: "uint8", bytes: []byte{0xC0, 0x7B}, bytes_read: 2, str: "123"},
		{name: "uint16", bytes: []byte{0xC1, 0x39, 0x30}, bytes_read: 3, str: "12345"},
		{name: "uint32", bytes: []byte{0xC2, 0x87, 0xD6, 0x12, 0x00}, bytes_read: 5, str: "1234567"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes_read, str := readEncodedString(test.bytes)
			if bytes_read != test.bytes_read || str != test.str {
				t.Errorf("Expected: bytes_read = %d, str: %s\nGot: bytes_read: %d, str: %s",
					test.bytes_read, test.str, bytes_read, str,
				)
			}
		})
	}
}

func TestReadRDBList(t *testing.T) {

	tests := []struct {
		name       string
		bytes      []byte
		bytes_read uint64
		list       []string
	}{
		{name: "simple list", bytes: []byte{0x02, 0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72}, bytes_read: 9, list: []string{"foo", "bar"}},
		{name: "empty list", bytes: []byte{0x00}, bytes_read: 1, list: []string{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes_read, list := readRDBList(test.bytes)
			if bytes_read != test.bytes_read || !equalSlices(list, test.list) {
				t.Errorf("Expected: bytes_read = %d, list: %v\nGot: bytes_read: %d, list: %v",
					test.bytes_read, test.list, bytes_read, list,
				)
			}
		})
	}
}

func TestReadRDBSet(t *testing.T) {

	tests := []struct {
		name       string
		bytes      []byte
		bytes_read uint64
		set        map[string]struct{}
	}{
		{name: "simple set", bytes: []byte{0x02, 0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72}, bytes_read: 9, set: map[string]struct{}{"foo": struct{}{}, "bar": struct{}{}}},
		{name: "empty set", bytes: []byte{0x00}, bytes_read: 1, set: map[string]struct{}{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bytes_read, set := readRDBSet(test.bytes)
			if bytes_read != test.bytes_read || !equalSets(set, test.set) {
				t.Errorf("Expected: bytes_read = %d, set: %v\nGot: bytes_read: %d, set: %v",
					test.bytes_read, test.set, bytes_read, set,
				)
			}
		})
	}
}

func equalSets(a map[string]struct{}, b map[string]struct{}) bool {
	for key := range a {
		if _, ok := b[key]; !ok {
			return false
		}
	}
	for key := range b {
		if _, ok := a[key]; !ok {
			return false
		}
	}
	return true
}

func equalSlices(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
