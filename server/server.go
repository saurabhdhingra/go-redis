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
				store.SET(key, val, expiration)
				resp.Respond(conn, resp.Value{Type: "string", Str: "OK"})
			nextCommand:
				continue

			case "GET":
				if len(args) < 1 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'get' command"})
					continue
				}
				key := args[0].Bulk
				val, found := store.GET(key)
				if found {
					resp.Respond(conn, resp.Value{Type: "bulk", Bulk: val})
				} else {
					resp.Respond(conn, resp.Value{Type: "nil"})
				}
			case "LPUSH":
				if len(args) < 2 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'lpush' command"})
					continue
				}
				key := args[0].Bulk
				elements := make([]string, len(args)-1)
				for i := 1; i < len(args); i++ {
					elements[i-1] = args[i].Bulk
				}
				newLen, err := store.LPUSH(key, elements)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
				} else {
					resp.Respond(conn, resp.Value{Type: "integer", Num: newLen})
				}
			case "LRANGE":
				if len(args) != 3 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'lrange' command"})
					continue
				}
				key := args[0].Bulk
				start, err := strconv.Atoi(args[1].Bulk)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"})
					continue
				}
				end, err := strconv.Atoi(args[2].Bulk)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"})
					continue
				}
				list, err := store.LRANGE(key, start, end)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
					continue
				}
				respValues := make([]resp.Value, len(list))
				for i, item := range list {
					respValues[i] = resp.Value{Type: "bulk", Bulk: item}
				}
				resp.Respond(conn, resp.Value{Type: "array", Array: respValues})
			case "LLEN":
				if len(args) != 1 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'llen' command"})
					continue
				}
				key := args[0].Bulk
				length, err := store.LLEN(key)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
					continue
				}
				resp.Respond(conn, resp.Value{Type: "integer", Num: length})
			case "LPOP":
				if len(args) != 1 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'lpop' command"})
					continue
				}
				key := args[0].Bulk
				element, found, err := store.LPOP(key)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
					continue
				}
				if found {
					resp.Respond(conn, resp.Value{Type: "bulk", Bulk: element})
				} else {
					resp.Respond(conn, resp.Value{Type: "nil"})
				}
			case "BLPOP":
				if len(args) < 2 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'blpop' command"})
					continue
				}
				keys := make([]string, len(args)-1)
				for i := 0; i < len(args)-1; i++ {
					keys[i] = args[i].Bulk
				}
				timeoutStr := args[len(args)-1].Bulk
				timeoutSeconds, err := strconv.Atoi(timeoutStr)
				if err != nil || timeoutSeconds < 0 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR timeout must be a non-negative integer"})
					continue
				}
				timeout := time.Duration(timeoutSeconds) * time.Second
				result, poppedKey, err := store.BLPOP(keys, timeout)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
					continue
				}
				if result != nil {
					resp.Respond(conn, resp.Value{Type: "array", Array: []resp.Value{{Type: "bulk", Bulk: poppedKey}, {Type: "bulk", Bulk: result[1]}}})
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
