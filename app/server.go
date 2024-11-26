package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
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

	var store *core.Store
	store = new(core.Store)
	store.Init()
	rdb_file := filepath.Join(flags.dir, flags.dbfilename)

	store, err = rdb.ReadFile(rdb_file)
	if err != nil {
		return err
	}
	store.SetParam("dir", flags.dir)
	store.SetParam("dbfilename", flags.dbfilename)

	if flags.replicaof != "" {
		strs := strings.Split(flags.replicaof, " ")
		if len(strs) != 2 {
			return fmt.Errorf("malformed value for --replicaof flag")
		}
		ip_port := strings.Join(strs, ":")
		store.SetParam("replicaof", ip_port)

		store.Master = performMasterHandshake(flags.port, ip_port, store)

	} else {
		store.SetParam("master_replid", generateRandomID(40))
		store.SetParam("master_repl_offset", "0")
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

func handleConnection(conn net.Conn, store *core.Store) {
	defer conn.Close()
	new_conn := core.NewConn(conn, core.ConnRelationTypeEnum.NORMAL)
	go new_conn.Read()
	acceptCommands(new_conn, store)
}

func acceptCommands(conn *core.Conn, store *core.Store) {
	for {
		n, response := resp.Decode(conn.ByteChan)
		fmt.Printf("decoded %d bytes from %v\n", n, conn.Conn.RemoteAddr())

		call := getRespArrayCall(response)

		res := acceptCommand(call, conn, store)
		if res != nil {
			conn.Write(res.Encode())
		}

		command_name, _ := getCommandName(call)
		if command_name == "SET" {
			propagateToReplicas(call, store)
		}

		if store.Master != nil && conn.Conn == store.Master.Conn {
			conn.Mu.Lock()
			conn.Offset += n
			conn.Mu.Unlock()
		}
	}
}

func acceptCommand(command resp.Object, conn *core.Conn, store *core.Store) resp.Object {
	call := getRespArrayCall(command)
	return handleCommand(call, conn, store)
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

func generateCommand(strs ...string) resp.Array {
	arr := resp.Array(make([]resp.Object, len(strs)))
	for i := 0; i < len(arr); i++ {
		bulk_str := resp.BulkString(strs[i])
		arr[i] = &bulk_str
	}
	return arr
}

func performMasterHandshake(listening_port string, master_ip_port string, store *core.Store) *core.Conn {

	conn, err := net.Dial("tcp", master_ip_port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not connect to master")
		os.Exit(1)
	}

	master_conn := core.NewConn(conn, core.ConnRelationTypeEnum.MASTER)
	go master_conn.Read()

	ping := generateCommand("PING")
	master_conn.Write(ping.Encode())
	if !waitForResponse("PONG", master_conn.ByteChan) {
		fmt.Fprintf(os.Stderr, "failed to PING master")
		os.Exit(1)
	}
	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(ping.Encode())))

	replconf := generateCommand("REPLCONF", "listening-port", listening_port)
	master_conn.Write(replconf.Encode())
	if !waitForResponse("OK", master_conn.ByteChan) {
		fmt.Fprintf(os.Stderr, "first REPLCONF to master failed")
		os.Exit(1)
	}
	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(replconf.Encode())))

	replconf = generateCommand("REPLCONF", "capa", "psync2")
	master_conn.Write(replconf.Encode())
	if !waitForResponse("OK", master_conn.ByteChan) {
		fmt.Fprintf(os.Stderr, "second REPLCONF to master failed")
		os.Exit(1)
	}
	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(replconf.Encode())))

	psync := generateCommand("PSYNC", "?", "-1")
	master_conn.Write(psync.Encode())
	_, raw := resp.Decode(master_conn.ByteChan)
	res, ok := resp.ToString(raw)
	if !ok {
		fmt.Fprintf(os.Stderr, "response is not a string")
		os.Exit(1)
	}
	strs := strings.Split(string(res), " ")
	if len(strs) != 3 || strs[0] != "FULLRESYNC" || strs[2] != "0" {
		fmt.Fprintf(os.Stderr, "malformed response to PSYNC command")
		os.Exit(1)
	}

	fmt.Printf("Sent command to master: %s\n", strconv.Quote(string(psync.Encode())))

	if <-master_conn.ByteChan != '$' {
		fmt.Fprintf(os.Stderr, "expected an RDB file\n")
		os.Exit(1)
	}
	_, raw_int := resp.DecodeInteger(master_conn.ByteChan)
	n := int(raw_int)
	for i := 0; i < n; i++ {
		<-master_conn.ByteChan
	}

	go acceptCommands(master_conn, store)

	return master_conn
}

func waitForResponse(response string, in <-chan byte) bool {
	_, actual := resp.Decode(in)
	str, ok := resp.ToString(actual)
	return ok && string(str) == response
}
