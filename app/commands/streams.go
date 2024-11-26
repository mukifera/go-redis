package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

func handleXaddCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) < 3 {
		return resp.SimpleError("invalid number of arguments to XADD command")
	}

	key, ok := resp.ToString(call[1])
	if !ok {
		return resp.SimpleError("expected a string stream key")
	}

	raw_stream, ok := store.Get(key)
	stream := &resp.Stream{}
	if ok {
		stream, ok = raw_stream.(*resp.Stream)
		if !ok {
			return resp.SimpleError("key has a non stream value type")
		}
	}

	id, ok := resp.ToString(call[2])
	if !ok {
		return resp.SimpleError("expected a string entry id")
	}

	id, err := processStreamID(stream, id)
	if err != nil {
		res := resp.SimpleError(err.Error())
		return res
	}

	data := make(map[string]resp.Object)
	if (len(call)-3)%2 != 0 {
		return resp.SimpleError("expected a list of key/value pairs")
	}

	for i := 3; i < len(call); i += 2 {
		data_key, ok := resp.ToString(call[i])
		if !ok {
			return resp.SimpleError("expected stream entry keys to be strings")
		}
		data[data_key] = call[i+1]
	}

	stream.Mu.Lock()
	stream.AddEntry(id, data)
	stream.Mu.Unlock()
	store.Set(key, stream)

	res := resp.BulkString(id)
	return res
}

func handleXrangeCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) != 4 {
		return resp.SimpleError("ERR invalid number of arguments to XRANGE command")
	}

	key, ok := resp.ToString(call[1])
	if !ok {
		res := resp.SimpleError("ERR expected a string key")
		return res
	}

	stream_raw, ok := store.Get(key)
	if !ok {
		return resp.SimpleError("ERR key does not exist in store")
	}
	stream, ok := stream_raw.(*resp.Stream)
	if !ok {
		return resp.SimpleError("ERR key does not hold a stream value")
	}

	stream.Mu.Lock()
	defer stream.Mu.Unlock()

	from_id, ok := resp.ToString(call[2])
	if !ok {
		return resp.SimpleError("ERR the start argument is not a valid string")
	}

	to_id, ok := resp.ToString(call[3])
	if !ok {
		return resp.SimpleError("ERR the end argument is not a valid string")
	}

	var from_index int
	if from_id == "-" {
		from_index = 0
	} else {
		from_index = streamLowerBound(stream, from_id)
	}

	var to_index int
	if to_id == "+" {
		to_index = len(stream.Entries) - 1
	} else {
		to_index = streamUpperBound(stream, to_id)
	}

	res := resp.Array{}
	for i := from_index; i <= to_index; i++ {
		entry := resp.Array{}
		entry = append(entry, resp.BulkString(stream.Entries[i].Id))

		data := resp.Array{}
		for k, v := range stream.Entries[i].Data {
			data = append(data, resp.BulkString(k))
			data = append(data, v)
		}

		entry = append(entry, data)
		res = append(res, entry)
	}

	return res
}

func handleXreadCommand(call resp.Array, conn *core.Conn, store *core.Store) resp.Object {
	if len(call) < 4 || len(call)%2 != 0 {
		return resp.SimpleError("ERR invalid number of arguments to XREAD command")
	}

	keys_and_ids := call[2:]
	sub, ok := resp.ToString(call[1])
	var timer <-chan time.Time

	is_blocking := ok && sub == "block"

	if is_blocking {
		keys_and_ids = call[4:]

		timeout, ok := resp.ToInt(call[2])
		if !ok {
			return resp.SimpleError("ERR expected timeout to be a number")
		}

		if timeout == 0 {
			timer = nil
		} else {
			timer = time.After(time.Duration(timeout) * time.Millisecond)
		}
	}

	num_of_streams := len(keys_and_ids) / 2

	keys := make([]string, 0)
	streams := make([]*resp.Stream, 0)
	ids := make([]string, 0)

	for i := 0; i < num_of_streams; i++ {
		key, ok := resp.ToString(keys_and_ids[i])
		if !ok {
			return resp.SimpleError("ERR expected a string for stream key")
		}

		id, ok := resp.ToString(keys_and_ids[i+num_of_streams])
		if !ok {
			return resp.SimpleError("ERR expected a string for stream key")
		}

		stream_raw, ok := store.Get(key)
		if !ok {
			return resp.SimpleError("ERR key does not exist in store")
		}
		stream, ok := stream_raw.(*resp.Stream)
		if !ok {
			return resp.SimpleError("ERR key does not hold a stream value")
		}

		if id == "$" {
			stream.Mu.Lock()
			id = stream.Entries[len(stream.Entries)-1].Id
			stream.Mu.Unlock()
		}

		keys = append(keys, key)
		streams = append(streams, stream)
		ids = append(ids, id)

	}

	var res resp.Object
	if is_blocking {
		res = blockStreamsRead(keys, streams, ids, timer)
	} else {
		res = readFromStreams(keys, streams, ids)
	}

	return res
}

func blockStreamsRead(keys []string, streams []*resp.Stream, ids []string, timer <-chan time.Time) resp.Object {
	for {
		select {
		case <-timer:
			return resp.NullBulkString{}
		default:
			reads := resp.Array{}
			for i := 0; i < len(streams); i++ {
				key := keys[i]
				stream := streams[i]
				id := ids[i]

				entries := readStreamEntries(stream, id)

				if len(entries) == 0 {
					continue
				}

				stream_read := resp.Array{}
				stream_read = append(stream_read, resp.BulkString(key))
				stream_read = append(stream_read, entries)

				reads = append(reads, stream_read)
			}

			if len(reads) != 0 {
				return reads
			}
		}
	}
}

func readFromStreams(keys []string, streams []*resp.Stream, ids []string) resp.Array {
	reads := resp.Array{}

	for i := 0; i < len(streams); i++ {
		key := keys[i]
		stream := streams[i]
		id := ids[i]

		read := readFromStream(key, stream, id)

		reads = append(reads, read)
	}

	return reads
}

func readFromStream(key string, stream *resp.Stream, id string) resp.Array {
	entries := readStreamEntries(stream, id)

	stream_read := resp.Array{}
	stream_read = append(stream_read, resp.BulkString(key))
	stream_read = append(stream_read, entries)

	return stream_read
}

func readStreamEntries(stream *resp.Stream, id string) resp.Array {
	stream.Mu.Lock()
	defer stream.Mu.Unlock()

	from_index := streamLowerBound(stream, id)
	if from_index < len(stream.Entries) && stream.Entries[from_index].Id == id {
		from_index++
	}

	entries := resp.Array{}
	for j := from_index; j < len(stream.Entries); j++ {
		entry := resp.Array{}
		entry = append(entry, resp.BulkString(stream.Entries[j].Id))

		data := resp.Array{}
		for k, v := range stream.Entries[j].Data {
			data = append(data, resp.BulkString(k))
			data = append(data, v)
		}

		entry = append(entry, data)
		entries = append(entries, entry)
	}

	return entries
}

func streamLowerBound(stream *resp.Stream, id string) int {
	low := -1
	high := len(stream.Entries)
	for low+1 < high {
		mid := (low + high) / 2
		if compareStreamIDs(id, stream.Entries[mid].Id) != 1 {
			high = mid
		} else {
			low = mid
		}
	}
	return high
}

func streamUpperBound(stream *resp.Stream, id string) int {
	low := -1
	high := len(stream.Entries)
	for low+1 < high {
		mid := (low + high) / 2
		if compareStreamIDs(id, stream.Entries[mid].Id) != -1 {
			low = mid
		} else {
			high = mid
		}
	}
	return low
}

func processStreamID(stream *resp.Stream, id string) (string, error) {

	if id == "*" {
		current_timestamp := time.Now().UnixMilli()
		return strconv.Itoa(int(current_timestamp)) + "-0", nil
	}

	top_id := "0-0"
	if len(stream.Entries) != 0 {
		top_id = stream.Entries[len(stream.Entries)-1].Id
	}

	id_split := strings.Split(id, "-")
	if len(id_split) != 2 {
		return "", fmt.Errorf("invalid id format")
	}

	id_ms, err := strconv.Atoi(id_split[0])
	if err != nil {
		return "", fmt.Errorf("ERR The ID %s does not have a valid timestamp", id)
	}

	if id_split[1] == "*" {

		top_split := strings.Split(top_id, "-")
		top_ms, _ := strconv.Atoi(top_split[0])
		top_seq, _ := strconv.Atoi(top_split[1])

		if id_ms < top_ms {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if id_ms == top_ms {
			id = id_split[0] + "-" + strconv.Itoa(top_seq+1)
		} else {
			id = id_split[0] + "-0"
		}

		return id, nil
	}

	_, err = strconv.Atoi(id_split[1])
	if err != nil {
		return "", fmt.Errorf("ERR The ID %s does not have a valid sequence number", id)
	}

	if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	} else if compareStreamIDs(id, top_id) != 1 {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return id, nil
}

func compareStreamIDs(a string, b string) int {
	if a == b {
		return 0
	}
	a_split := strings.Split(a, "-")
	b_split := strings.Split(b, "-")

	a_ms, _ := strconv.Atoi(a_split[0])
	a_seq, _ := strconv.Atoi(a_split[1])
	b_ms, _ := strconv.Atoi(b_split[0])
	b_seq, _ := strconv.Atoi(b_split[1])

	if a_ms == b_ms {
		if a_seq < b_seq {
			return -1
		}
		return 1
	}
	if a_ms < b_ms {
		return -1
	}
	return 1
}
