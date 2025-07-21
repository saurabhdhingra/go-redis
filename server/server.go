package server

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/saurabhdhingra/go-redis/resp"
	"github.com/saurabhdhingra/go-redis/store"
)

func HandleConnection(conn net.Conn, store *store.KeyValueStore) {
	defer conn.Close()

	respReader := resp.NewResp(conn)

	for {
		value, err := respReader.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
			} else {
				fmt.Println("Error reading RESP:", err)
			}

			return
		}

		if value.Type == "array" && len(value.Array) > 0 {
			command := strings.ToUpper(value.Array[0].Bulk)
			args := value.Array[1:]

			switch command {
			case "PING":
				resp.Respond(conn, resp.Value{Type: "string", Str: "PONG"})
			case "ECHO":
				if len(args) > 0 {
					resp.Respond(conn, resp.Value{Type: "bulk", Bulk: args[0].Bulk})
				} else {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguements for 'echo' command"})
				}
			case "SET":
				if len(args) < 2 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguements for 'set' command"})
					continue
				}
				key := args[0].Bulk
				val := args[1].Bulk
				expiration := time.Time{}

				for i := 2; i < len(args); i++ {
					option := strings.ToUpper(args[i].Bulk)
					switch option {
					case "EX":
						if i+1 >= len(args) {
							resp.Respond(conn, resp.Value{Type: "error", Str: "ERR syntax error"})
							goto nextCommand
						}
						seconds, err := strconv.Atoi(args[i+1].Bulk)
						if err != nil || seconds <= 0 {
							resp.Respond(conn, resp.Value{Type: "error", Str: "ERR invalid expire time in 'set' command"})
							goto nextCommand
						}
						expiration = time.Now().Add(time.Duration(seconds) * time.Second)
						i++
					case "PX":
						if i+1 >= len(args) {
							resp.Respond(conn, resp.Value{Type: "error", Str: "ERR syntax error"})
						}
						milliseconds, err := strconv.Atoi(args[i+1].Bulk)
						if err != nil || milliseconds <= 0 {
							resp.Respond(conn, resp.Value{Type: "error", Str: "ERR invalid expire time in 'set' command"})
							goto nextCommand
						}
						expiration = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
						i++
					default:
						resp.Respond(conn, resp.Value{Type: "error", Str: "ERR syntax error"})
						goto nextCommand
					}
				}
				store.Set(key, val, expiration)
				resp.Respond(conn, resp.Value{Type: "string", Str: "OK"})
			nextCommand:
				continue

			case "GET":
				if len(args) < 1 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"})
					continue
				}
				key := args[0].Bulk
				val, found := store.Get(key)
				if found {
					resp.Respond(conn, resp.Value{Type: "bulk", Bulk: val})
				} else {
					resp.Respond(conn, resp.Value{Type: "nil"})
				}
			default:
				resp.Respond(conn, resp.Value{Type: "error", Str: "ERR unknown command '" + command + "'"})
			}
		} else {
			resp.Respond(conn, resp.Value{Type: "error", Str: "ERR invalid command format"})
		}
	}
}
