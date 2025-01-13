package cmd

import (
	"strconv"
	"sync"

	"github.com/IAmRiteshKoushik/bluedis/resp"
	"github.com/IAmRiteshKoushik/bluedis/store"
)

var sortedSetStore = make(map[string]*store.SortedSet[string, int64, string])
var sortedSetStoreMu sync.Mutex

func getOrCreateSortedSet(key string) *store.SortedSet[string, int64, string] {
    sortedSetStoreMu.Lock()
    defer sortedSetStoreMu.Unlock()
    zset, exists := sortedSetStore[key]
    if !exists {
        zset = store.NewSortedSet[string, int64, string]()
        sortedSetStore[key] = zset
    }
    return zset
}

func Zadd(args []resp.Value) resp.Value {
    if len(args) < 3 || len(args)%2 != 1 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'zadd' command"}
    }
    key := args[0].Bulk
    zset := getOrCreateSortedSet(key)
    sortedSetStoreMu.Lock()
    defer sortedSetStoreMu.Unlock()
    count := 0
    for i := 1; i < len(args); i += 2 {
        score, err := strconv.ParseInt(args[i].Bulk, 10, 64)
        if err != nil {
            return resp.Value{Typ: "error", Str: "ERR invalid score value for 'zadd' command"}
        }
        member := args[i+1].Bulk
        _, exists := zset.Dict[member]
        zset.AddOrUpdate(member, score, member)
        if !exists {
            count++
        }
    }
    return resp.Value{Typ: "integer", Num: count}
}

func Zrem(args []resp.Value) resp.Value {
    if len(args) < 2 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'zrem' command"}
    }
    key := args[0].Bulk
    sortedSetStoreMu.Lock()
    zset, exists := sortedSetStore[key]
    sortedSetStoreMu.Unlock()

    if !exists {
        return resp.Value{Typ: "integer", Num: 0}
    }
    sortedSetStoreMu.Lock()
    defer sortedSetStoreMu.Unlock()
    count := 0
    for i := 1; i < len(args); i++ {
        member := args[i].Bulk
        if _, exists := zset.Dict[member];
		exists {
            zset.Remove(member)
            count++
        }
    }
    return resp.Value{Typ: "integer", Num: count}
}

func Zrange(args []resp.Value) resp.Value {
    if len(args) != 3 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'zrange' command"}
    }
    key := args[0].Bulk
    start, err1 := strconv.Atoi(args[1].Bulk)
    end, err2 := strconv.Atoi(args[2].Bulk)
    if err1 != nil || err2 != nil {
        return resp.Value{Typ: "error", Str: "ERR invalid range values for 'zrange' command"}
    }
    sortedSetStoreMu.Lock()
    zset, exists := sortedSetStore[key]
    sortedSetStoreMu.Unlock()
    if !exists {
        return resp.Value{Typ: "array", Array: []resp.Value{}}
    }
    sortedSetStoreMu.Lock()
    defer sortedSetStoreMu.Unlock()
    members := zset.GetRangeByRank(start, end, false)
    result := make([]resp.Value, 0, len(members))
    for _, member := range members {
        result = append(result, resp.Value{Typ: "bulk", Bulk: member.Value})
    }
    return resp.Value{Typ: "array", Array: result}
}

func ZupdateScore(args []resp.Value) resp.Value {
    if len(args) != 3 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'zupdateScore' command"}
    }
    key := args[0].Bulk
    member := args[1].Bulk
    newScore, err := strconv.ParseInt(args[2].Bulk, 10, 64)
    if err != nil {
        return resp.Value{Typ: "error", Str: "ERR invalid score value for 'zupdateScore' command"}
    }
    sortedSetStoreMu.Lock()
    zset, exists := sortedSetStore[key]
    sortedSetStoreMu.Unlock()
    if !exists {
        return resp.Value{Typ: "error", Str: "ERR sorted set does not exist"}
    }
    sortedSetStoreMu.Lock()
    defer sortedSetStoreMu.Unlock()
    if _, exists := zset.Dict[member]; exists {
        zset.AddOrUpdate(member, newScore, member)
        return resp.Value{Typ: "string", Str: "OK"}
    }
    return resp.Value{Typ: "error", Str: "ERR member does not exist in sorted set"}
}


func ZtopK(args []resp.Value) resp.Value {
    if len(args) != 2 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'ztopK' command"}
    }
    key := args[0].Bulk
    k, err := strconv.Atoi(args[1].Bulk)
    if err != nil || k <= 0 {
        return resp.Value{Typ: "error", Str: "ERR invalid value for K"}
    }
    sortedSetStoreMu.Lock()
    zset, exists := sortedSetStore[key]
    sortedSetStoreMu.Unlock()
    if !exists {
        return resp.Value{Typ: "array", Array: []resp.Value{}}
    }
    sortedSetStoreMu.Lock()
    defer sortedSetStoreMu.Unlock()
    members := zset.GetRangeByRank(0, k, false)
	result := make([]resp.Value, 0, len(members))
    for _, member := range members {
        result = append(result, resp.Value{Typ: "bulk", Bulk: member.Value})
    }
    return resp.Value{Typ: "array", Array: result}
}
func Zranktop(args []resp.Value) resp.Value {
    if len(args) != 2 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'zranktop' command"}
    }
    key := args[0].Bulk
    member := args[1].Bulk
    sortedSetStoreMu.Lock()
    zset, exists := sortedSetStore[key]
    sortedSetStoreMu.Unlock()
    if !exists {
        return resp.Value{Typ: "error", Str: "ERR sorted set does not exist"}
    }
    rank, found := zset.FindRank(member, true)
    if !found {
        return resp.Value{Typ: "error", Str: "ERR member does not exist in sorted set"}
    }
    return resp.Value{Typ: "integer", Num: rank}
}
func Zrankbottom(args []resp.Value) resp.Value {
    if len(args) != 2 {
        return resp.Value{Typ: "error", Str: "ERR wrong number of arguments for 'zrankbottom' command"}
    }
    key := args[0].Bulk
    member := args[1].Bulk
    sortedSetStoreMu.Lock()
    zset, exists := sortedSetStore[key]
    sortedSetStoreMu.Unlock()
    if !exists {
        return resp.Value{Typ: "error", Str: "ERR sorted set does not exist"}
    }
    rank, found := zset.FindRank(member, false)
    if !found {
        return resp.Value{Typ: "error", Str: "ERR member does not exist in sorted set"}
    }
    return resp.Value{Typ: "integer", Num: rank }
}