package cmd

import "github.com/IAmRiteshKoushik/bluedis/resp"

func Ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Typ: "string", Str: "PONG"}
	}
	return resp.Value{Typ: "string", Str: args[0].Bulk}
}
