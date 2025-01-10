package cmd

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/IAmRiteshKoushik/bluedis/resp"
	"github.com/IAmRiteshKoushik/bluedis/store"
)

var BitMapStore = make(map[string]*store.StringBitMap)
var BitMapStoreMu sync.Mutex

func SetBit(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'setbit' command"}
	}

	key := args[0].Bulk
	pos, err := strconv.ParseUint(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Value{Typ: "error", Str: "ERR invalid position argument for 'setbit' command"}
	}
	value, err := strconv.Atoi(args[2].Bulk)
	if err != nil || (value != 0 && value != 1) {
		return resp.Value{Typ: "error", Str: "ERR invalid value argument for 'setbit' command"}
	}

	BitMapStoreMu.Lock()
	defer BitMapStoreMu.Unlock()

	bitmap, exists := BitMapStore[key] 
	if !exists {
		bitmap = store.NewStringBitMap()
		BitMapStore[key] = bitmap 
	}
	err = bitmap.SetBit(key, pos, value == 1)
	if err != nil {
		return resp.Value{Typ: "error", Str: fmt.Sprintf("ERR %v", err)}
	}

	return resp.Value{Typ: "integer", Num: 1}
}

func GetBit(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'getbit' command"}
	}

	key := args[0].Bulk
	pos, err := strconv.ParseUint(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Value{Typ: "error", Str: "ERR invalid position argument for 'getbit' command"}
	}

	BitMapStoreMu.Lock()
	defer BitMapStoreMu.Unlock()

	bitmap, exists := BitMapStore[key] 
	if !exists {
		 return resp.Value{Typ: "integer", Num: 0} 
	}
	value, err := bitmap.GetBit(key, pos)
	if err != nil {
		return resp.Value{Typ: "error", Str: fmt.Sprintf("ERR %v", err)}
	}

	if value {
		return resp.Value{Typ: "integer", Num: 1}
	}
	return resp.Value{Typ: "integer", Num: 0}
}

func BitCount(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'bitcount' command"}
	}

	key := args[0].Bulk

	BitMapStoreMu.Lock()
	defer BitMapStoreMu.Unlock()

	bitmap, exists := BitMapStore[key] 
	if !exists {
		 return resp.Value{Typ: "integer", Num: 0} 
	}
	count, err := bitmap.PopCount(key)
	if err != nil {
		return resp.Value{Typ: "error", Str: fmt.Sprintf("ERR %v", err)}
	}

	return resp.Value{Typ: "integer", Num: count}
}

func DelBitMap(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'delbit' command"}
	}

	key := args[0].Bulk

	BitMapStoreMu.Lock()
	defer BitMapStoreMu.Unlock()

	delete(BitMapStore, key)
	
	return resp.Value{Typ: "integer", Num: 1}
}
