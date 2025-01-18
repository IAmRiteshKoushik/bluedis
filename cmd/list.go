package cmd

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/IAmRiteshKoushik/bluedis/resp"
	"github.com/IAmRiteshKoushik/bluedis/store"
)

var ListStore = make(map[string]*store.DoublyLinkedList)
var ListStoreMu sync.Mutex

func Lpush(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpush' command"}
	}

	key := args[0].Bulk
	elements := args[1:]

	ListStoreMu.Lock()
	list, exists := ListStore[key]
	if !exists {
		list = store.NewDoublyLinkedList()
		ListStore[key] = list
	}
	for _, element := range elements {
		list.PushLeft(element.Bulk)
	}
	length := list.Length()
	ListStoreMu.Unlock()

	return resp.Value{Typ: "integer", Num: length}
}

func Rpush(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpush' command"}
	}

	key := args[0].Bulk
	elements := args[1:]

	ListStoreMu.Lock()
	list, exists := ListStore[key]
	if (!exists) {
		list = store.NewDoublyLinkedList()
		ListStore[key] = list
	}
	for _, element := range elements {
		list.PushRight(element.Bulk)
	}
	length := list.Length()
	ListStoreMu.Unlock()

	return resp.Value{
		Typ: "integer",
		Num: length,
	}
}

func Lpop(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lpop' command"}
	}

	key := args[0].Bulk
	count := 1 // Default to popping one element
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(args[1].Bulk)
		if err != nil || count <= 0 {
			return resp.Value{Typ: "error", Str: "ERR invalid count argument for 'lpop' command"}
		}
	}

	ListStoreMu.Lock()
	list, exists := ListStore[key]
	if !exists || list.Length() == 0 {
		ListStoreMu.Unlock()
		return resp.Value{Typ: "null"}
	}

	result := make([]resp.Value, 0, count)
	for i := 0; i < count && list.Length() > 0; i++ {
		value, ok := list.PopLeft()
		if !ok {
			ListStoreMu.Unlock()
			return resp.Value{Typ: "null"}
		}
		result = append(result, resp.Value{Typ: "bulk", Bulk: fmt.Sprintf("%v", value)})
	}
	ListStoreMu.Unlock()

	if len(result) == 1 {
		return result[0]
	}
	return resp.Value{
		Typ:   "array",
		Array: result,
	}
}

func Rpop(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'rpop' command"}
	}

	key := args[0].Bulk
	count := 1
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(args[1].Bulk)
		if err != nil || count <= 0 {
			return resp.Value{Typ: "error", Str: "ERR invalid count argument for 'rpop' command"}
		}
	}

	ListStoreMu.Lock()
	list, exists := ListStore[key]
	if !exists || list.Length() == 0 {
		ListStoreMu.Unlock()
		return resp.Value{Typ: "null"}
	}

	result := make([]resp.Value, 0, count)
	for i := 0; i < count && list.Length() > 0; i++ {
		value, _ := list.PopRight()
		result = append(result, resp.Value{Typ: "bulk", Bulk: fmt.Sprintf("%v", value)})
	}
	ListStoreMu.Unlock()

	if len(result) == 1 {
		return result[0]
	}
	return resp.Value{
		Typ:   "array",
		Array: result,
	}
}

func Llen(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'llen' command"}
	}

	key := args[0].Bulk

	ListStoreMu.Lock()
	list, exists := ListStore[key]
	length := 0
	if exists {
		length = list.Length()
	}
	ListStoreMu.Unlock()

	return resp.Value{
		Typ: "integer",
		Num: length,
	}
}

func Lrange(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'lrange' command"}
	}

	key := args[0].Bulk
	start, err1 := strconv.Atoi(args[1].Bulk)
	end, err2 := strconv.Atoi(args[2].Bulk)
	if err1 != nil || err2 != nil {
		return resp.Value{Typ: "error", Str: "ERR invalid arguments for 'lrange' command"}
	}

	ListStoreMu.Lock()
	list, exists := ListStore[key]
	if !exists {
		ListStoreMu.Unlock()
		return resp.Value{
			Typ:   "array",
			Array: []resp.Value{},
		}
	}

	values := list.ExtractRange(start, end)
	result := make([]resp.Value, len(values))
	for i, v := range values {
		result[i] = resp.Value{Typ: "bulk", Bulk: fmt.Sprintf("%v", v)}
	}
	ListStoreMu.Unlock()

	return resp.Value{
		Typ:   "array",
		Array: result,
	}
}

func Blpop(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'blpop' command"}
	}

	// Extract keys and timeout.
	keys := args[:len(args)-1]
	timeout, err := strconv.Atoi(args[len(args)-1].Bulk)
	if err != nil || timeout < 0 {
		return resp.Value{Typ: "error", Str: "ERR invalid timeout argument for 'blpop' command"}
	}

	// Create a ticker for polling and a timer for timeout.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var timer *time.Timer
	var timerC <-chan time.Time

	if timeout > 0 {
		timer = time.NewTimer(time.Duration(timeout) * time.Second)
		defer timer.Stop()
		timerC = timer.C
	}

	for {
		// Check all keys under lock.
		ListStoreMu.Lock()
		for _, key := range keys {
			list, exists := ListStore[key.Bulk]
			if exists && list.Length() > 0 {
				value, _ := list.PopLeft()
				ListStoreMu.Unlock()

				return resp.Value{
					Typ: "array",
					Array: []resp.Value{
						{Typ: "bulk", Bulk: key.Bulk},
						{Typ: "bulk", Bulk: fmt.Sprintf("%v", value)},
					},
				}
			}
		}
		ListStoreMu.Unlock()

		// Wait for either timeout or next tick.
		select {
		case <-timerC:
			return resp.Value{Typ: "null"}
		case <-ticker.C:
			// Continue to next iteration.
		}

		// If timeout is 0, return immediately.
		if timeout == 0 {
			return resp.Value{Typ: "null"}
		}
	}
}
