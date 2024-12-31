package cmd

import (
	"log"
	"strconv"
	"sync"

	"github.com/IAmRiteshKoushik/bluedis/resp"
	"github.com/IAmRiteshKoushik/bluedis/store"
)

var bloomStore = make(map[string]*store.BloomFilter)
var bloomStoreMu sync.RWMutex

func BFReserve(args []resp.Value) resp.Value {
	log.Println(args)
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'BF.RESERVE' command"}
	}
	key := args[0].Bulk
	capacity := args[1].Bulk
	_, exists := bloomStore[key]
	if exists {
		return resp.Value{Typ: "error", Str: "ERR key already exists"}
	}

	size, err := strconv.Atoi(capacity)
	if err != nil {
		return resp.Value{Typ: "error", Str: "ERR capacity must be an integer"}
	}
	bloomStoreMu.Lock()
	bloomStore[key] = store.NewBloomFilter(size)
	bloomStoreMu.Unlock()
	return resp.Value{Typ: "string", Str: "OK"}
}

func BFAdd(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'BF.ADD' command"}
	}
	key := args[0].Bulk
	item := args[1]

	// If filer doesn't exist, make it
	bloomStoreMu.Lock()
	filter, exists := bloomStore[key]
	if !exists {
		// Default Size of 10000 bytes
		filter = store.NewBloomFilter(10000)
		bloomStore[key] = filter
	}
	bloomStoreMu.Unlock()

	// If item alr exists, return 0 (could be wrong, false positive)
	// Otherwise add the item and return 1
	if filter.Exists(item) {
		return resp.Value{
			Typ: "integer",
			Num: 0,
		}
	}

	bloomStoreMu.Lock()
	filter.Add(item)
	bloomStoreMu.Unlock()
	return resp.Value{
		Typ: "integer",
		Num: 1,
	}
}

func BFExists(args []resp.Value) resp.Value {
	bloomStoreMu.RLock()
	defer bloomStoreMu.RUnlock()

	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'BF.EXISTS' command"}
	}
	key := args[0].Bulk
	value := args[1]
	filter, exists := bloomStore[key]
	// If the filter doesn't exist, retrun 0
	if !exists {
		return resp.Value{
			Typ: "integer",
			Num: 0,
		}
	}

	// If value exists in the bloomfilter, return 1 otherwise 0
	if filter.Exists(value) {
		return resp.Value{
			Typ: "integer",
			Num: 1,
		}
	}
	return resp.Value{
		Typ: "integer",
		Num: 0,
	}
}

// A Mix between BF.RESERVE and BF.MADD
func insertItems(args []resp.Value, start int, filter *store.BloomFilter) resp.Value {
	bloomStoreMu.Lock()
	defer bloomStoreMu.Unlock()
	resultArray := resp.Value{
		Typ: "array",
	}
	for i := start; i < len(args); i++ {
		value := args[i]
		log.Println(value)
		if filter.Exists(value) {
			resultArray.Array = append(resultArray.Array, resp.Value{
				Typ: "integer",
				Num: 0,
			})
		} else {
			filter.Add(value)
			resultArray.Array = append(resultArray.Array, resp.Value{
				Typ: "integer",
				Num: 1,
			})
		}
	}
	return resultArray
}
func BFInsert(args []resp.Value) resp.Value {
	log.Println(args)
	if len(args) < 4 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'BF.INSERT' command"}
	}
	key := args[0].Bulk
	optArgument, optVal := args[1].Bulk, args[2].Bulk
	bloomStoreMu.Lock()
	filter, exists := bloomStore[key]
	if !exists {
		switch optArgument {
		case "NOCREATE":
			return resp.Value{
				Typ:   "array",
				Array: make([]resp.Value, 0),
			}

		case "CAPACITY":
			capacity, err := strconv.Atoi(optVal)
			if err != nil {
				return resp.Value{
					Typ:   "array",
					Array: make([]resp.Value, 0),
				}
			}
			filter = store.NewBloomFilter(capacity)
			bloomStore[key] = filter

		case "ITEMS":
			// Creating default filter
			filter = store.NewBloomFilter(10000)
			bloomStore[key] = filter

		// IF any other argument,
		default:
			return resp.Value{
				Typ:   "array",
				Array: make([]resp.Value, 0),
			}
		}

	}
	bloomStoreMu.Unlock()
	// Loop through to find ITEMS
	for index, element := range args {
		if index == 0 {
			continue
		}
		_, err := strconv.Atoi(element.Bulk)
		if err != nil {
			switch element.Bulk {
			case "NOCREATE":
				continue
			case "CAPACITY":
				continue
			case "ITEMS":
				return insertItems(args, index+1, filter)
			default:
				// Any other argument is not allowed
				return resp.Value{
					Typ:   "array",
					Array: make([]resp.Value, 0),
				}
			}
		}
	}

	return resp.Value{
		Typ:   "array",
		Array: make([]resp.Value, 0),
	}
}

func BFMAdd([]resp.Value) resp.Value {
	return resp.Value{}
}

func BFMExists([]resp.Value) resp.Value {
	return resp.Value{}
}
