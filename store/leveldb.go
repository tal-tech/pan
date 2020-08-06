/**
 *Created by He Haitao at 2020/6/10 5:57 下午
 */
package store

import (
	"pan/meta"

	"github.com/beeker1121/goque"
	"github.com/tal-tech/xtools/confutil"
)

type LevelDB struct {
	writerOffset int
	readerOffset int
	stype        string
	lqueue       *goque.Queue
}

func init() {
	if stype == meta.LEVELDB {
		AddStorer(stype, InitLevelDB)
	}
}

func InitLevelDB() (Storer, error) {
	this := new(LevelDB)
	var err error
	this.lqueue, err = goque.OpenQueue(confutil.GetConf("Pan", "levelDB"))
	this.stype = meta.LEVELDB
	return this, err
}

func (this *LevelDB) Push(msg []byte) error {
	_, err := this.lqueue.Enqueue(msg)
	if err != nil {
		return err
	}
	this.writerOffset += int(len(msg) + 2)
	return nil
}

func (this *LevelDB) Pop() ([]byte, error) {
	item, err := this.lqueue.Dequeue()
	if err != nil {
		return nil, err
	}
	ret := item.Value
	this.readerOffset += int(2 + len(ret))
	return ret, nil
}

func (this *LevelDB) GetWriterOffset() int {
	return this.writerOffset
}

func (this *LevelDB) GetReaderOffset() int {
	return this.readerOffset
}

func (this *LevelDB) GetStorerType() string {
	return this.stype
}
