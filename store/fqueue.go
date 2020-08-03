package store

import (
	"pan/meta"

	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"

	"github.com/Joinhack/fqueue"
)

type FileQueue struct {
	queue *fqueue.FQueue
	stype string
}

func init() {
	if stype == meta.FILE {
		AddStorer(stype, InitFileQueue)
	}
}

func InitFileQueue() (Storer, error) {
	fq := new(FileQueue)
	fqueue.QueueLimit = 1024 * 1024 * 1024 * 4
	var err error
	fq.queue, err = fqueue.NewFQueue(confutil.GetConf("Pan", "path"))
	fq.stype = meta.FILE
	return fq, err
}

func (this *FileQueue) Push(msg []byte) error {
	return this.queue.Push(msg)
}

func (this *FileQueue) Pop() ([]byte, error) {
	return this.queue.Pop()
}

func (this *FileQueue) GetWriterOffset() int {
	return this.queue.WriterOffset
}

func (this *FileQueue) GetReaderOffset() int {
	return this.queue.ReaderOffset
}

func (this *FileQueue) GetStorerType() string {
	return this.stype
}
