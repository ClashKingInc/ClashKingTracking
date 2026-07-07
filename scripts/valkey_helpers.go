package scripts

import "github.com/valkey-io/valkey-go"

func cursorCommand(client valkey.Client, key, cursor string) valkey.Completed {
	if cursor == "" {
		return client.B().Del().Key(key).Build()
	}
	return client.B().Set().Key(key).Value(cursor).Build()
}
