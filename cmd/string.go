package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IAmRiteshKoushik/bluedis/resp"
)

var SETs = make(map[string]Values)
var SETsMu = sync.RWMutex{}

func Set(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Value{
			Typ: "error",
			Str: "ERR wrong number of arguments for 'set' command",
		}
	}

	key := args[0].Bulk
	value := Values{Content: args[1].Bulk}
	expiry := false

	for i := 2; i < len(args); i += 2 {
		if i+1 < len(args) {
			switch strings.ToUpper(args[i].Bulk) {
			case "PX":
				expiry = true
				ms, err := strconv.ParseInt(args[i+1].Bulk, 10, 64)
				if err != nil {
					return resp.Value{Typ: "error", Str: "ERR invalid PX value"}
				}
				value.Begone = time.Now().Add(time.Duration(ms) * time.Millisecond)
			case "EX":
				expiry = true
				s, err := strconv.Atoi(args[i+1].Bulk)
				if err != nil {
					return resp.Value{Typ: "error", Str: "ERR invalid EX value"}
				}
				value.Begone = time.Now().Add(time.Duration(s) * time.Second)
			}
		}
	}

	value.HasExpiry = expiry

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	fmt.Printf("SET: key=%s, value=%s, expiry=%v, Begone=%v\n", key, value.Content, value.HasExpiry, value.Begone)

	return resp.Value{Typ: "string", Str: "OK"}
}

func ExpireHandler(args []resp.Value) resp.Value {
	if len(args) < 2 || len(args) > 3 {
		return resp.Value{
			Typ: "error",
			Str: "ERR wrong number of arguments for 'expire' command",
		}
	}

	key := args[0].Bulk
	seconds, err := strconv.Atoi(args[1].Bulk)
	if err != nil {
		return resp.Value{
			Typ: "error",
			Str: "ERR value is not an integer or out of range",
		}
	}

	var flag string
	if len(args) == 3 {
		flag = strings.ToUpper(args[2].Bulk)
		if flag != "NX" && flag != "XX" && flag != "GT" && flag != "LT" {
			return resp.Value{
				Typ: "error",
				Str: "ERR invalid flag value",
			}
		}
	}

	SETsMu.Lock()
	defer SETsMu.Unlock()
	value, ok := SETs[key]
	if !ok {
		return resp.Value{Typ: "integer", Num: 0} // Key does not exist
	}

	now := time.Now()
	newExpiry := now.Add(time.Duration(seconds) * time.Second)

	applyExpiry := false
	switch flag {
	case "":
		applyExpiry = true
	case "NX":
		if !value.HasExpiry {
			applyExpiry = true
		}
	case "XX":
		if value.HasExpiry {
			applyExpiry = true
		}
	case "GT":
		if !value.HasExpiry || newExpiry.After(value.Begone) {
			applyExpiry = true
		}
	case "LT":
		if !value.HasExpiry || newExpiry.Before(value.Begone) {
			applyExpiry = true
		}
	}

	fmt.Println("EXPIRE: key=", key, "expiryTime=", newExpiry, "SETs[key]=", SETs[key])

	if applyExpiry {
		value.HasExpiry = true
		value.Begone = newExpiry
		SETs[key] = value
		return resp.Value{Typ: "integer", Num: 1}
	}

	fmt.Println("EXPIRE: key=", key, "expiryTime=", newExpiry, "SETs[key]=", SETs[key])

	return resp.Value{Typ: "integer", Num: 0}
}

func Get(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{
			Typ: "error",
			Str: "ERR wrong number of arguments for 'get' command",
		}
	}

	key := args[0].Bulk
	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if ok && value.HasExpiry && time.Now().After(value.Begone) {
		// Key needs to be-gone for good
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()
		return resp.Value{Typ: "null"}
	}

	if !ok {
		return resp.Value{Typ: "null"}
	}

	return resp.Value{
		Typ:  "bulk",
		Bulk: value.Content,
	}
}

func Delete(args []resp.Value) resp.Value {
    if len(args) < 1 {
        return resp.Value{
            Typ: "error",
            Str: "ERR wrong number of arguments for 'del' command",
        }
    }
    deletedCount := 0
    for _, arg := range args {
        key := arg.Bulk

        // Remove key from SETs
        SETsMu.Lock()
        if _, ok := SETs[key]; ok {
            delete(SETs, key)
            fmt.Println("DEL: key=", key)
            deletedCount++
        }
        SETsMu.Unlock()

        // Remove key from listStore
        ListStoreMu.Lock()
        if _, ok := ListStore[key]; ok {
            delete(ListStore, key)
            fmt.Println("DEL: list key=", key)
            deletedCount++
        }
        ListStoreMu.Unlock()

		// Remove key from bitMapStore
		BitMapStoreMu.Lock()
		if _, ok := BitMapStore[key]; ok {
			delete(BitMapStore, key)
			fmt.Println("DEL: bitMap key=", key)
			deletedCount++
		}
		BitMapStoreMu.Unlock()
    }
    fmt.Println("DEL: deletedCount=", deletedCount)
    return resp.Value{
        Typ: "integer",
        Num: deletedCount,
    }
}
