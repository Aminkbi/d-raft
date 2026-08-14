package etcdraft

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"

	rootraft "github.com/aminkbi/d-raft/raft"
)

var ErrOracleOrder = errors.New("etcdraft: chain-of-blocks apply order mismatch")

const ChainSchemaVersion = "d-raft.etcdraft-chain/v1"

// Block commits to one applied log entry and the complete preceding chain.
type Block struct {
	Index      uint64 `json:"index"`
	Term       uint64 `json:"term"`
	EntryType  uint8  `json:"entry_type"`
	DataDigest string `json:"data_digest"`
	Digest     string `json:"digest"`
}

// Chain is a versioned adapter-side commitment to ordered applied-entry
// history. It includes implementation-specific indexes, terms, types, and
// no-ops. Equality is evidence of equal encoded histories under the hash
// assumption; it is not a proof of Raft safety or application correctness.
type Chain struct {
	blocks []Block
}

func (c *Chain) Apply(entry rootraft.Entry) error {
	want := uint64(1)
	var previous [sha256.Size]byte
	if len(c.blocks) > 0 {
		want = c.blocks[len(c.blocks)-1].Index + 1
		decoded, err := hex.DecodeString(c.blocks[len(c.blocks)-1].Digest)
		if err != nil || len(decoded) != sha256.Size {
			return ErrOracleOrder
		}
		copy(previous[:], decoded)
	}
	if entry.Index != want {
		return ErrOracleOrder
	}
	dataDigest := sha256.Sum256(entry.Data)
	hash := sha256.New()
	hash.Write([]byte(ChainSchemaVersion))
	hash.Write([]byte{0})
	hash.Write(previous[:])
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], entry.Index)
	hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], entry.Term)
	hash.Write(number[:])
	hash.Write([]byte{byte(entry.Type)})
	hash.Write(dataDigest[:])
	c.blocks = append(c.blocks, Block{Index: entry.Index, Term: entry.Term, EntryType: uint8(entry.Type), DataDigest: hex.EncodeToString(dataDigest[:]), Digest: hex.EncodeToString(hash.Sum(nil))})
	return nil
}

func (c *Chain) Blocks() []Block { return slices.Clone(c.blocks) }

func (c *Chain) Digest() string {
	if len(c.blocks) == 0 {
		return hex.EncodeToString(make([]byte, sha256.Size))
	}
	return c.blocks[len(c.blocks)-1].Digest
}

func (c *Chain) Index() uint64 {
	if len(c.blocks) == 0 {
		return 0
	}
	return c.blocks[len(c.blocks)-1].Index
}
