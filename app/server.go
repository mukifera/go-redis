package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type serverFlags struct {
	dir        string
	dbfilename string
	port       string
	replicaof  string
}

func main() {
	dir_ptr := flag.String("dir", "", "the directory of the RDB config file")
	dbfilename_ptr := flag.String("dbfilename", "", "the name of the RDB config file")
	port_ptr := flag.String("port", "6379", "the port to run the server on")
	replicaof_ptr := flag.String("replicaof", "", "indicate if the server is a replica of another. In the form of '<MASTER_HOST> <MASTER_PORT>'")
	flag.Parse()

	err := startServer(serverFlags{
		dir:        *dir_ptr,
		dbfilename: *dbfilename_ptr,
		port:       *port_ptr,
		replicaof:  *replicaof_ptr,
	}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func startServer(flags serverFlags, stop <-chan struct{}) error {

	l, err := net.Listen("tcp", "0.0.0.0:"+flags.port)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s", flags.port)
	}
	defer l.Close()

	var store *redisStore
	store = new(redisStore)
	store.init()
	rdb_file := filepath.Join(flags.dir, flags.dbfilename)

	store, err = readRDBFile(rdb_file)
	if err != nil {
		return err
	}
	store.setParam("dir", flags.dir)
	store.setParam("dbfilename", flags.dbfilename)

	if flags.replicaof != "" {
		strs := strings.Split(flags.replicaof, " ")
		if len(strs) != 2 {
			return fmt.Errorf("malformed value for --replicaof flag")
		}
		ip_port := strings.Join(strs, ":")
		store.setParam("replicaof", ip_port)

		store.master = performMasterHandshake(flags.port, ip_port, store)

	} else {
		store.setParam("master_replid", generateRandomID(40))
		store.setParam("master_repl_offset", "0")
	}

	for {
		select {
		case <-stop:
			return nil
		default:
			conn, err := l.Accept()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error accepting connection: %v\n", err)
				continue
			}
			go handleConnection(conn, store)

		}
	}
}

func handleConnection(conn net.Conn, store *redisStore) {
	defer conn.Close()
	new_conn := newRedisConn(conn, connRelationTypeEnum.NORMAL)
	go readFromConnection(new_conn)
	acceptCommands(new_conn, store)
}

func acceptCommands(conn *redisConn, store *redisStore) {
	for {
		n, response := decode(conn.byteChan)
		fmt.Printf("decoded %d bytes from %v\n", n, conn.conn.RemoteAddr())

		if store.master != nil && conn.conn == store.master.conn {
			conn.mu.Lock()
			conn.offset += n
			conn.mu.Unlock()
		}

		switch res := response.(type) {
		case respArray:
			handleCommand(res, conn, store)
		case respSimpleString, respBulkString:
			call := []respObject{res}
			handleCommand(call, conn, store)
		default:
			fmt.Fprintf(os.Stderr, "invalid command %v\n", response)
		}
	}
}

func writeToConnection(conn *redisConn, data []byte) {
	current := 0
	for current < len(data) {
		n, err := conn.conn.Write(data[current:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write to connection: %v\n", err)
			return
		}
		current += n
	}
	if conn.relation == connRelationTypeEnum.REPLICA {
		conn.mu.Lock()
		conn.total_propagated += len(data)
		fmt.Printf("sent %d bytes to replica %v: %s\n", len(data), conn.conn.RemoteAddr(), strconv.Quote(string(data)))
		fmt.Printf("total_propagated = %d, offset = %d\n", conn.total_propagated, conn.offset)
		conn.mu.Unlock()
	} else {
		fmt.Printf("sent %d bytes to %v: %s\n", len(data), conn.conn.RemoteAddr(), strconv.Quote(string(data)))
	}
}

func readFromConnection(conn *redisConn) {
	defer close(conn.byteChan)

	for {
		buf := make([]byte, 1024)
		n, err := conn.conn.Read(buf)
		if err == io.EOF {
			continue
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read from connection: %v\n", err)
			return
		}
		fmt.Printf("read %d bytes from %v: %s\n", n, conn.conn.RemoteAddr(), strconv.Quote(string(buf[:n])))
		for i := 0; i < n; i++ {
			conn.byteChan <- buf[i]
		}
	}
}

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func generateRandomID(length int) string {
	const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var sb strings.Builder
	for i := 0; i < length; i++ {
		sb.WriteByte(alpha[seededRand.Intn(len(alpha))])
	}
	return sb.String()
}

func generateCommand(strs ...string) []byte {
	arr := respArray(make([]respObject, len(strs)))
	for i := 0; i < len(arr); i++ {
		bulk_str := respBulkString(strs[i])
		arr[i] = &bulk_str
	}
	return arr.encode()
}

func performMasterHandshake(listening_port string, master_ip_port string, store *redisStore) *redisConn {

	conn, err := net.Dial("tcp", master_ip_port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not connect to master")
		os.Exit(1)
	}

	master_conn := newRedisConn(conn, connRelationTypeEnum.MASTER)
	go readFromConnection(master_conn)

	ping := generateCommand("PING")
	writeToConnection(master_conn, ping)
	if !waitForResponse("PONG", master_conn.byteChan) {
		fmt.Fprintf(os.Stderr, "failed to PING master")
		os.Exit(1)
	}
	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(ping)))

	replconf := generateCommand("REPLCONF", "listening-port", listening_port)
	writeToConnection(master_conn, replconf)
	if !waitForResponse("OK", master_conn.byteChan) {
		fmt.Fprintf(os.Stderr, "first REPLCONF to master failed")
		os.Exit(1)
	}
	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(replconf)))

	replconf = generateCommand("REPLCONF", "capa", "psync2")
	writeToConnection(master_conn, replconf)
	if !waitForResponse("OK", master_conn.byteChan) {
		fmt.Fprintf(os.Stderr, "second REPLCONF to master failed")
		os.Exit(1)
	}
	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(replconf)))

	psync := generateCommand("PSYNC", "?", "-1")
	writeToConnection(master_conn, psync)
	_, raw := decode(master_conn.byteChan)
	res, ok := respToString(raw)
	if !ok {
		fmt.Fprintf(os.Stderr, "response is not a string")
		os.Exit(1)
	}
	strs := strings.Split(string(res), " ")
	if len(strs) != 3 || strs[0] != "FULLRESYNC" || strs[2] != "0" {
		fmt.Fprintf(os.Stderr, "malformed response to PSYNC command")
		os.Exit(1)
	}

	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(psync)))

	if <-master_conn.byteChan != '$' {
		fmt.Fprintf(os.Stderr, "expected an RDB file\n")
		os.Exit(1)
	}
	_, raw_int := decodeInteger(master_conn.byteChan)
	n := int(raw_int)
	for i := 0; i < n; i++ {
		<-master_conn.byteChan
	}

	go acceptCommands(master_conn, store)

	return master_conn
}

func waitForResponse(response string, in <-chan byte) bool {
	_, actual := decode(in)
	str, ok := respToString(actual)
	return ok && string(str) == response
}
