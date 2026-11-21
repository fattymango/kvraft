// Copy of internal/kv/store/txn.go; unchanged.
package store

import "kvapp/internal/kv/proto"

type DedupEntry struct {
	Seq   int64
	Reply proto.TxnReply
}
