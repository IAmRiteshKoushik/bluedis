package cmd

import (
	"sync"

	"github.com/IAmRiteshKoushik/bluedis/resp"
)

var HSETs = make(map[string]map[string]string)
var HSETsMu = sync.RWMutex{}

func Hset(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{
			Typ: "error",
			Str: "ERR wrong number of arguments for 'hset' command",
		}
	}

	hash := args[0].Bulk
	key := args[1].Bulk
	value := args[2].Bulk

	HSETsMu.Lock()
	defer HSETsMu.Unlock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = make(map[string]string)
	}
	HSETs[hash][key] = value

	return resp.Value{Typ: "string", Str: "OK"}
}

func Hget(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{
			Typ: "error",
			Str: "ERR wrong number of arguments for 'hget' command",
		}
	}

	hash := args[0].Bulk
	key := args[1].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{
		Typ:  "bulk",
		Bulk: value,
	}
}

func Hgetall(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Typ: "error",
			Str: "ERR wrong number of arguments for 'hgetall' command",
		}
	}

	hash := args[0].Bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return resp.Value{Typ: "null"}
	}

	resps := []resp.Value{}
	for k, v := range value {
		resps = append(resps, resp.Value{Typ: "bulk", Bulk: k})
		resps = append(resps, resp.Value{Typ: "bulk", Bulk: v})
	}

	return resp.Value{
		Typ:   "array",
		Array: resps,
	}
}
