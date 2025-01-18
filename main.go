package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/IAmRiteshKoushik/bluedis/aof"
	"github.com/IAmRiteshKoushik/bluedis/cmd"
	"github.com/IAmRiteshKoushik/bluedis/resp"
	"github.com/IAmRiteshKoushik/bluedis/store"

)

func main() {

	// Creating a new server / listener
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Listening on PORT: 6379")

	aof, err := aof.NewAof("database.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	// Persistance added and database automatically reconstructs from AOF
	aof.Read(func(value resp.Value) {
		if value.Typ == "array" && len(value.Array) > 0 {
			command := strings.ToUpper(value.Array[0].Bulk)
			args := value.Array[1:]

			switch command {
			case "SET":
				if len(args) >= 2 {
					key := args[0].Bulk
					val := args[1].Bulk
					cmd.SETsMu.Lock()
					currentVal := cmd.Values{Content: val, HasExpiry: false}
					cmd.SETs[key] = currentVal
					cmd.SETsMu.Unlock()
					// Handle EX/PX during reconstruction
					for i := 2; i < len(args); i += 2 {
						if i+1 < len(args) {
							switch strings.ToUpper(args[i].Bulk) {
							case "EX":
								seconds, _ := strconv.Atoi(args[i+1].Bulk)
								cmd.SETsMu.Lock()
								currentVal := cmd.SETs[key]
								currentVal.Begone = time.Now().Add(time.Duration(seconds) * time.Second)
								currentVal.HasExpiry = true
								cmd.SETs[key] = currentVal
								cmd.SETsMu.Unlock()
							case "PX":
								milliseconds, _ := strconv.ParseInt(args[i+1].Bulk, 10, 64)
								cmd.SETsMu.Lock()
								currentVal := cmd.SETs[key]
								currentVal.Begone = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
								currentVal.HasExpiry = true
								cmd.SETs[key] = currentVal
								cmd.SETsMu.Unlock()
							}
						}
					}
				}
			case "EXPIRE":
				if len(args) >= 2 {
					key := args[0].Bulk
					seconds, _ := strconv.Atoi(args[1].Bulk)
					expiryTime := time.Now().Add(time.Duration(seconds) * time.Second)
					cmd.SETsMu.Lock()
					fmt.Println("EXPIRE: key=", key, "expiryTime=", expiryTime, "SETs[key]=", cmd.SETs[key])
					if val, ok := cmd.SETs[key]; ok {
						val.HasExpiry = true
						val.Begone = expiryTime
						cmd.SETs[key] = val
					}
					cmd.SETsMu.Unlock()
				}
			case "DEL":
				for _, arg := range args {
					cmd.SETsMu.Lock()
					delete(cmd.SETs, arg.Bulk)
					cmd.SETsMu.Unlock()
				}
			case "LPUSH", "RPUSH":
				if len(args) >= 2 {
					key := args[0].Bulk
					
					cmd.ListStoreMu.Lock()
					list, exists := cmd.ListStore[key]
					if !exists {
						list = store.NewDoublyLinkedList()
						cmd.ListStore[key] = list
					}
					
					if command == "LPUSH" {
						// For LPUSH, process values in reverse order to maintain correct order
						for i := len(args) - 1; i >= 1; i-- {
							list.PushLeft(args[i].Bulk)
						}
					} else { // RPUSH
						// For RPUSH, process values in forward order
						for i := 1; i < len(args); i++ {
							list.PushRight(args[i].Bulk)
						}
					}
					
					cmd.ListStoreMu.Unlock()
				}
			case "LPOP", "RPOP":
				if len(args) >= 1 {
					key := args[0].Bulk
					count := 1
					if len(args) >= 2 {
						parsedCount, err := strconv.Atoi(args[1].Bulk)
						if err == nil && parsedCount > 0 {
							count = parsedCount
						}
					}
					
					cmd.ListStoreMu.Lock()
					if list, exists := cmd.ListStore[key]; exists {
						for i := 0; i < count && list.Length() > 0; i++ {
							if command == "LPOP" {
								list.PopLeft()
							} else {
								list.PopRight()
							}
						}
						// Remove the key if list is empty
						if list.Length() == 0 {
							delete(cmd.ListStore, key)
						}
					}
					cmd.ListStoreMu.Unlock()
				}
			
			case "BLPOP":
				if len(args) >= 2 {
					key := args[0].Bulk
					cmd.ListStoreMu.Lock()
					list, exists := cmd.ListStore[key]
					if exists && list.Length() > 0 {
						list.BlockingPopLeft()
						// Remove the key if list is empty
						if list.Length() == 0 {
							delete(cmd.ListStore, key)
						}
					}
					cmd.ListStoreMu.Unlock()
				}
			case "SETBIT":
				if len(args) == 3 {
					key := args[0].Bulk
					pos, _ := strconv.ParseUint(args[1].Bulk, 10, 64)
					value, _ := strconv.Atoi(args[2].Bulk)
					cmd.BitMapStoreMu.Lock()
					bitmap, exists := cmd.BitMapStore[key]
					if !exists {
						bitmap = store.NewStringBitMap()
						cmd.BitMapStore[key] = bitmap
					}
					bitmap.SetBit(key, pos, value == 1)
					cmd.BitMapStoreMu.Unlock()
				}
			
			case "GETBIT":
				if len(args) == 2 {
					key := args[0].Bulk
					pos, _ := strconv.ParseUint(args[1].Bulk, 10, 64)
					cmd.BitMapStoreMu.Lock()
					bitmap, exists := cmd.BitMapStore[key]
					if exists {
						bitmap.GetBit(key, pos)
					}
					cmd.BitMapStoreMu.Unlock()
				}
			
			case "BITCOUNT":
				if len(args) == 1 {
					key := args[0].Bulk
					cmd.BitMapStoreMu.Lock()
					bitmap, exists := cmd.BitMapStore[key]
					if exists {
						bitmap.PopCount(key)
					}
					cmd.BitMapStoreMu.Unlock()
				}
			}
		}
	})

	// When a connection drops, we continue listening for a new connection
	for {
		// Listening for new connections (this is a blocking connection) and whenever
		// a connection is made then an acceptance is established using Accept()
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()

		// Writer allocation for writing back to redis-cli
		writer := resp.NewWriter(conn)

		// Create an infinite for-loop so that we can keep listening to the port
		// constantly, receive commands from clients and respond to them
		for {
			response := resp.NewResp(conn)
			value, err := response.Read()
			if err != nil {
				if err == io.EOF {
					fmt.Println("Client disconnected from Bluedis server.")
					break
				}
				fmt.Println(err)
				break
			}

			if value.Typ != "array" {
				fmt.Println("Invalid request, expected array")
				continue
			}

			if len(value.Array) == 0 {
				fmt.Println("Invalid request, expected array length > 0")
				continue
			}

			command := strings.ToUpper(value.Array[0].Bulk)
			args := value.Array[1:]

			handler, ok := cmd.Handlers[command]
			// Redis sends an initial command when connecting, handling it
			if command == "COMMAND" || command == "RETRY" {
				fmt.Println("Client connected to Bluedis server!")
				writer.Write(resp.Value{Typ: "string", Str: ""})
				continue
			}
			if !ok {
				fmt.Println("Invalid command: ", command)
				writer.Write(resp.Value{Typ: "string", Str: ""})

				continue
			}

			if command == "EXPIRE" {
				// Expire command
				result := cmd.ExpireHandler(args)
				fmt.Println(args)
				if result.Typ == "integer" && result.Num == 1 {
					num, err := strconv.Atoi(args[1].Bulk)
					aof.WriteExpire(args[0].Bulk, num, args[2].Bulk) // Write EXPIRE to AOF if successful
					if err != nil {
						fmt.Println(err)
					}
				}
				writer.Write(result)
				continue

				// expire(args)
			}

			if command == "DEL" {
				result := cmd.Delete(args)
				if result.Typ == "integer" && result.Num > 0 {
					keys := make([]string, len(args))
					for i, arg := range args {
						keys[i] = arg.Bulk
					}
					aof.WriteDel(keys) // DEL to AOF if successful
				}
				writer.Write(result)
				continue
			}

			// Append "write" commands to AOF
			if command == "SET" || command == "HSET" || command == "LPUSH" || command == "RPUSH" || command == "SETBIT" {
				aof.Write(value)
			}

			result := handler(args)
			err = writer.Write(result)
			if err != nil {
				fmt.Println(err)
			}
		}
	}

}
