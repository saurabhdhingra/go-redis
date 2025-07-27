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

	// Transaction state per connection
	inTransaction := false
	var queuedCommands [][]resp.Value

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

			// Transaction handling
			if inTransaction && command != "EXEC" && command != "DISCARD" && command != "MULTI" {
				// Queue the command
				queuedCommands = append(queuedCommands, value.Array)
				resp.Respond(conn, resp.Value{Type: "string", Str: "QUEUED"})
				continue
			}

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
			case "TYPE":
				if len(args) != 1 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'type' command"})
					continue
				}
				key := args[0].Bulk
				typeStr := store.TYPE(key)
				resp.Respond(conn, resp.Value{Type: "string", Str: typeStr})

			case "XADD":
				if len(args) < 3 || (len(args)-2)%2 != 0 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'xadd' command"})
					continue
				}
				key := args[0].Bulk
				id := args[1].Bulk
				fields := make(map[string]string)
				for i := 2; i < len(args); i += 2 {
					fields[args[i].Bulk] = args[i+1].Bulk
				}
				newID, err := store.XADD(key, id, fields)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
				} else {
					resp.Respond(conn, resp.Value{Type: "bulk", Bulk: newID})
				}

			case "XRANGE":
				if len(args) < 3 || len(args) > 5 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'xrange' command"})
					continue
				}
				key := args[0].Bulk
				start := args[1].Bulk
				end := args[2].Bulk
				count := 0
				if len(args) == 5 && strings.ToUpper(args[3].Bulk) == "COUNT" {
					count, _ = strconv.Atoi(args[4].Bulk)
				}
				entries, err := store.XRANGE(key, start, end, count)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
					continue
				}
				respEntries := make([]resp.Value, len(entries))
				for i, entry := range entries {
					fields := []resp.Value{{Type: "bulk", Bulk: entry.ID}}
					for k, v := range entry.Fields {
						fields = append(fields, resp.Value{Type: "bulk", Bulk: k})
						fields = append(fields, resp.Value{Type: "bulk", Bulk: v})
					}
					respEntries[i] = resp.Value{Type: "array", Array: fields}
				}
				resp.Respond(conn, resp.Value{Type: "array", Array: respEntries})

			case "XREAD":
				// Example: XREAD COUNT 2 STREAMS mystream 0-0
				count := 0
				block := time.Duration(0)
				var streamsIdx int
				for i := 0; i < len(args); i++ {
					if strings.ToUpper(args[i].Bulk) == "COUNT" && i+1 < len(args) {
						count, _ = strconv.Atoi(args[i+1].Bulk)
						i++
					} else if strings.ToUpper(args[i].Bulk) == "BLOCK" && i+1 < len(args) {
						ms, _ := strconv.Atoi(args[i+1].Bulk)
						block = time.Duration(ms) * time.Millisecond
						i++
					} else if strings.ToUpper(args[i].Bulk) == "STREAMS" {
						streamsIdx = i
						break
					}
				}
				if streamsIdx == 0 || streamsIdx+1 >= len(args) {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR syntax error in 'xread' command"})
					continue
				}
				streamNames := []string{}
				ids := []string{}
				for i := streamsIdx + 1; i < len(args); i++ {
					if i < streamsIdx+1+(len(args)-streamsIdx-1)/2 {
						streamNames = append(streamNames, args[i].Bulk)
					} else {
						ids = append(ids, args[i].Bulk)
					}
				}
				if len(streamNames) != len(ids) {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR number of streams and ids do not match in 'xread' command"})
					continue
				}
				streamsMap := make(map[string]string)
				for i := 0; i < len(streamNames); i++ {
					streamsMap[streamNames[i]] = ids[i]
				}
				entriesMap, err := store.XREAD(streamsMap, count, block)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: err.Error()})
					continue
				}
				respStreams := []resp.Value{}
				for stream, entries := range entriesMap {
					respEntries := make([]resp.Value, len(entries))
					for i, entry := range entries {
						fields := []resp.Value{{Type: "bulk", Bulk: entry.ID}}
						for k, v := range entry.Fields {
							fields = append(fields, resp.Value{Type: "bulk", Bulk: k})
							fields = append(fields, resp.Value{Type: "bulk", Bulk: v})
						}
						respEntries[i] = resp.Value{Type: "array", Array: fields}
					}
					respStreams = append(respStreams, resp.Value{Type: "array", Array: []resp.Value{
						{Type: "bulk", Bulk: stream},
						{Type: "array", Array: respEntries},
					}})
				}
				resp.Respond(conn, resp.Value{Type: "array", Array: respStreams})
			case "INCR":
				if len(args) != 1 {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'incr' command"})
					continue
				}
				key := args[0].Bulk
				val, found := store.GET(key)
				if !found {
					store.SET(key, "1", time.Time{})
					resp.Respond(conn, resp.Value{Type: "integer", Num: 1})
					continue
				}
				intVal, err := strconv.Atoi(val)
				if err != nil {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"})
					continue
				}
				intVal++
				store.SET(key, strconv.Itoa(intVal), time.Time{})
				resp.Respond(conn, resp.Value{Type: "integer", Num: intVal})

			case "MULTI":
				if inTransaction {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR MULTI calls can not be nested"})
					continue
				}
				inTransaction = true
				queuedCommands = nil
				resp.Respond(conn, resp.Value{Type: "string", Str: "OK"})

			case "EXEC":
				if !inTransaction {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR EXEC without MULTI"})
					continue
				}
				results := make([]resp.Value, len(queuedCommands))
				for i, cmdArr := range queuedCommands {
					if len(cmdArr) == 0 {
						results[i] = resp.Value{Type: "error", Str: "ERR empty command in transaction"}
						continue
					}
					cmd := strings.ToUpper(cmdArr[0].Bulk)
					cmdArgs := cmdArr[1:]
					// Simulate command execution (only for supported commands)
					switch cmd {
					case "INCR":
						if len(cmdArgs) != 1 {
							results[i] = resp.Value{Type: "error", Str: "ERR wrong number of arguments for 'incr' command"}
							continue
						}
						key := cmdArgs[0].Bulk
						val, found := store.GET(key)
						if !found {
							store.SET(key, "1", time.Time{})
							results[i] = resp.Value{Type: "integer", Num: 1}
							continue
						}
						intVal, err := strconv.Atoi(val)
						if err != nil {
							results[i] = resp.Value{Type: "error", Str: "ERR value is not an integer or out of range"}
							continue
						}
						intVal++
						store.SET(key, strconv.Itoa(intVal), time.Time{})
						results[i] = resp.Value{Type: "integer", Num: intVal}
					// Add more supported commands here as needed
					default:
						results[i] = resp.Value{Type: "error", Str: "ERR command not supported in transaction"}
					}
				}
				inTransaction = false
				queuedCommands = nil
				resp.Respond(conn, resp.Value{Type: "array", Array: results})

			case "DISCARD":
				if !inTransaction {
					resp.Respond(conn, resp.Value{Type: "error", Str: "ERR DISCARD without MULTI"})
					continue
				}
				inTransaction = false
				queuedCommands = nil
				resp.Respond(conn, resp.Value{Type: "string", Str: "OK"})
			default:
				resp.Respond(conn, resp.Value{Type: "error", Str: "ERR unknown command '" + command + "'"})
			}
		} else {
			resp.Respond(conn, resp.Value{Type: "error", Str: "ERR invalid command format"})
		}
	}
}
