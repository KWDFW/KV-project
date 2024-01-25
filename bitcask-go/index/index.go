package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Indexer抽象索引接口，后序如果想要接入其他的数据结构，则直接实现这个接口即可
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

// Item BTree中需要的
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 方法，实现Btree中的Less接口，对key进行排列
func (ai *Item) Less(bi btree.Item) bool {
	// 比较两个字节数组，如果a小于b，则返回-1，即返回true
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
