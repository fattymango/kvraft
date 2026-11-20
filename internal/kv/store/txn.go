package store

import "kvapp/internal/kv/proto"

type DedupEntry struct {
	Seq   int64
	Reply proto.TxnReply
}
