package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	// "github.com/codecrafters-io/redis-starter-go/app/resp" // Our custom RESP package
	"github.com/saurabhdhingra/go-redis/server" // Our server package
	"github.com/saurabhdhingra/go-redis/store"  // Our key-value store package
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	// Parse flags
	port := flag.String("port", "6379", "Port to listen on")
	replicaof := flag.String("replicaof", "", "host:port of master (if this is a replica)")
	flag.Parse()

	role := "master"
	masterAddr := ""
	if *replicaof != "" {
		role = "slave"
		masterAddr = *replicaof
	}

	// Initialize the global key-value store
	store := store.NewKeyValueStore()

	l, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", *port)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		// Pass role and master info to the handler
		go server.HandleConnectionWithRole(conn, store, role, masterAddr)
	}
}
