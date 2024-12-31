package cmd

import (
	"time"

	"github.com/IAmRiteshKoushik/bluedis/resp"
)

var Handlers = map[string]func([]resp.Value) resp.Value{
	"PING":       Ping,
	"SET":        Set,
	"GET":        Get,
	"HSET":       Hset,
	"HGET":       Hget,
	"HGETALL":    Hgetall,
	"LPUSH":      Lpush,
	"LPOP":       Lpop,
	"RPUSH":      Rpush,
	"RPOP":       Rpop,
	"LLEN":       Llen,
	"LRANGE":     Lrange,
	"BLPOP":      Blpop,
	"EXPIRE":     ExpireHandler,
	"DEL":        Delete,
	"BF.ADD":     BFAdd,
	"BF.EXISTS":  BFExists,
	"BF.MADD":    BFMAdd,
	"BF.MEXISTS": BFMExists,
	"BF.INSERT":  BFInsert,
	"BF.RESERVE": BFReserve,
}

type Values struct {
	Content   string
	Begone    time.Time
	HasExpiry bool
}
