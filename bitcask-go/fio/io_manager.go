package fio

const DataFilePerm = 0644

// IOManger 抽象 IO 管理接口，可以接入不同的 IO 类型，目前支持标准文件 IO

type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 将内存缓冲区的数据持久化到磁盘当中
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
}

// NewIOmanger 初始化 IOManger，目前只支持标准FileIO
func NewIOManger(fileName string) (IOManager, error) {
	return NewFileIOManger(fileName)
}
