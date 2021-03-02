package nodenameresolver

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/hashicorp/raft"
)

func getHash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func Resolve(address string) raft.ServerID {
	return raft.ServerID(getHash(address))
}
