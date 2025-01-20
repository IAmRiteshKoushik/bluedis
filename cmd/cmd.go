package cmd

import (
	"strconv"
	"time"

	"github.com/IAmRiteshKoushik/bluedis/cuckoo"
	"github.com/IAmRiteshKoushik/bluedis/resp"
)

//adding cuckooFilters to track the memory
var cuckooFilters = make(map[string]*cuckoo.CuckooFilter)

var Handlers = map[string]func([]resp.Value) resp.Value{
    "PING":    Ping,
    "SET":     Set,
    "GET":     Get,
    "HSET":    Hset,
    "HGET":    Hget,
    "HGETALL": Hgetall,
    "LPUSH":   Lpush,
    "LPOP":    Lpop,
    "RPUSH":   Rpush,
    "RPOP":    Rpop,
    "LLEN":    Llen,
    "LRANGE":  Lrange,
    "BLPOP":   Blpop,
    "EXPIRE":  ExpireHandler,
    "DEL":     Delete,

    "CF.ADD":      CuckooAdd,
    "CF.CONTAINS": CuckooContains,
    "CF.REMOVE":   CuckooRemove,
    "CF.CREATE":   CuckooCreate,
	"CF.RESERVE": CfReserve, //for redis compatible
}

type Values struct {
    Content   string
    Begone    time.Time
    HasExpiry bool
}

//this creates a new cuckoo filter
func CuckooCreate(args []resp.Value) resp.Value {
    if len(args) < 2 {
        return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments for CF.CREATE"}
    }

    key := args[1].Bulk
    
    //to check if it already exists
    if _, exists := cuckooFilters[key]; exists {
        return resp.Value{Typ: "error", Str: "ERR filter already exists"}
    }

    //if no create a new filter with default configs
    cuckooFilters[key] = cuckoo.New(nil)
    return resp.Value{Typ: "string", Str: "OK"}
}

//adds an item to the filter
func CuckooAdd(args []resp.Value) resp.Value {
    if len(args) < 3 {
        return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments for CF.ADD"}
    }

    key := args[1].Bulk
    item := args[2].Bulk

    //create the filter
    filter, exists := cuckooFilters[key]
    if !exists {
        filter = cuckoo.New(nil)
        cuckooFilters[key] = filter
    }

    if filter.Add([]byte(item)) {
        return resp.Value{Typ: "string", Str: "OK"}
    }
    return resp.Value{Typ: "error", Str: "ERR filter is full"}
}

//checks if an item is already in the filter or not 
func CuckooContains(args []resp.Value) resp.Value {
    if len(args) < 3 {
        return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments for CF.CONTAINS"}
    }

    key := args[1].Bulk
    item := args[2].Bulk

    filter, exists := cuckooFilters[key]
    if !exists {
        return resp.Value{Typ: "integer", Num: 0}
    }

    if filter.Contains([]byte(item)) {
        return resp.Value{Typ: "integer", Num: 1}
    }
    return resp.Value{Typ: "integer", Num: 0}
}

//removes an item from the cuckoo filter
func CuckooRemove(args []resp.Value) resp.Value {
    if len(args) < 3 {
        return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments for CF.REMOVE"}
    }

    key := args[1].Bulk
    item := args[2].Bulk

    filter, exists := cuckooFilters[key]
    if !exists {
        return resp.Value{Typ: "error", Str: "ERR filter does not exist"}
    }

    if filter.Remove([]byte(item)) {
        return resp.Value{Typ: "string", Str: "OK"}
    }
    return resp.Value{Typ: "error", Str: "ERR item not found"}
}



func CfReserve(args []resp.Value) resp.Value {
    if len(args) < 3 {
        return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments for CF.RESERVE"}
    }

    key := args[1].Bulk
    capacity, err := strconv.ParseUint(args[2].Bulk, 10, 32)
    if err != nil {
        return resp.Value{Typ: "error", Str: "ERR invalid capacity"}
    }

    config := &cuckoo.Config{
        Capacity:   uint(capacity),
        BucketSize: 6,  //the default bucket size i kept in cuckoo.go
        MaxKicks:   700, //the default kick size i kept in cuckoo.go
    }

    //this is for parsing optional paramerts
    for i := 3; i < len(args); i += 2 {
        switch args[i].Bulk {
        case "BUCKETSIZE":
            if i+1 >= len(args) {
                return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments foR BUCKETSIZE"}
            }
            size, err := strconv.Atoi(args[i+1].Bulk)
            if err != nil {
                return resp.Value{Typ: "error", Str: "ERR not valid bucket size"}
            }
            config.BucketSize = size
        case "MAXITERATIONS":
            if i+1 >= len(args) {
                return resp.Value{Typ: "error", Str: "ERR Wrong no. of arguments for MAXITERATIONS"}
            }
            iter, err := strconv.Atoi(args[i+1].Bulk)
            if err != nil {
                return resp.Value{Typ: "error", Str: "ERR not vaid max iterations"}
            }
            config.MaxKicks = iter
        }
    }

    filter := cuckoo.New(config)
    cuckooFilters[key] = filter
    return resp.Value{Typ: "string", Str: "OK"}
}

