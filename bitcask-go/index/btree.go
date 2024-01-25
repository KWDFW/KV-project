package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

// BTree 索引，主要封装了google的btree库
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // RWMutex 读写锁
}

// NewBTree 初始化 BTree 索引结构
// NewBTree initializes a new BTree index structure.
// It creates a new btree with 32 leaf nodes and a RWMutex lock for concurrency.
// Returns a pointer to the initialized BTree.
func NewBTree() *BTree {
	return &BTree{
		// 参数为叶子节点的数量
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	// BTree库中写是并发不安全的，需要加锁，读是并发安全的，不需要加锁
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	// 如果原文件为空，则无效操作
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
