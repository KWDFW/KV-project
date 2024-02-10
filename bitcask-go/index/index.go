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

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// BTree索引
	Btree IndexType = iota + 1

	// ART自适应基数树索引
	ART
)

// NewIndexer根据类型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
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

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的key查到到第一个大于(或小于)等于目标的key，从这个key开始遍历
	Seek(key []byte)

	// Next 跳转到下一个key
	Next()

	// Valid是否有效，即是否已经遍历完了所有的key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的Key的数据
	Key() []byte

	// Value 当前遍历位置的Value数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
