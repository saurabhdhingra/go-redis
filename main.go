package main

import (
	"fmt"
	"net"
	"os"
	// "github.com/codecrafters-io/redis-starter-go/app/resp" // Our custom RESP package
	"github.com/saurabhdhingra/go-redis/server" // Our server package
	"github.com/saurabhdhingra/go-redis/store" // Our key-value store package
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	// Initialize the global key-value store
	store := store.NewKeyValueStore()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		// Handle each connection in a separate goroutine
		go server.HandleConnection(conn, store)
	}
}

