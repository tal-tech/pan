package internal

import (
	"sync"
	"time"

	"pan/meta"
	"pan/store"

	"git.100tal.com/wangxiao_go_lib/xesLogger"

	"github.com/Joinhack/fqueue"
	"github.com/beeker1121/goque"
)

type PChan struct {
	exit       chan struct{} //退出信号
	needStore  bool          //是否需要落盘
	storeMutex *sync.Mutex   //锁
	waiter     *sync.WaitGroup
	queue      store.Storer //数据容灾存储接口
	limit      int          //阈值
	input      chan []byte  //input管道，有缓冲型，大小 limit*1.5
	output     chan []byte  //output管道，有缓冲型l,大小 limit*1.5

	Input  chan<- []byte //外部使用的input,主要是给server使用，还有proxy失败回传
	Output <-chan []byte //外部使用的output，主要是给proxy使用，proxy从中读取消息
}

func NewPChan(length int64) (*PChan, error) {
	this := new(PChan)
	this.exit = make(chan struct{}, 1)
	this.waiter = new(sync.WaitGroup)
	this.limit = int(length)

	this.storeMutex = new(sync.Mutex)
	this.input = make(chan []byte, length+length/2)
	this.output = make(chan []byte, length+length/2)
	this.Input = this.input
	this.Output = this.output
	var err error
	this.queue, err = store.GetStorer()
	if err != nil {
		return nil, err
	}
	this.waiter.Add(3)
	go this.monitor()
	go this.dealInput()
	go this.restore()
	return this, nil
}

func (this *PChan) getStoreStatus() bool {
	this.storeMutex.Lock()
	defer this.storeMutex.Unlock()
	return this.needStore
}

func (this *PChan) updateStoreStatus(status bool) {
	this.storeMutex.Lock()
	c := false
	if this.needStore != status {
		this.needStore = status
		c = true
	}
	this.storeMutex.Unlock()
	if c {
		logger.I("StoreStatusChange", "needStore:%t", status)
	}
}

func (this *PChan) dealInput() {
	defer meta.Recovery()
	for {
		select {
		case msg := <-this.input:
			if len(this.output) >= this.limit {
				this.updateStoreStatus(true)
			}
			if this.getStoreStatus() == false {
				this.output <- msg
			} else {
				err := this.queue.Push(msg)
				if err != nil {
					logger.E("DealInput", "%v QueuePushFailed,err:%v,msg:%v", this.queue.GetStorerType(), err, msg)
				} else {
					logger.W("DealInput", "%v QueuePushSuccess,msg:%v", this.queue.GetStorerType(), msg)
				}
			}
		case <-this.exit:
			logger.I("DealInput", "EXIT dealInput")
			this.waiter.Done()
			return
		}
	}
}

func (this *PChan) restore() {
	defer meta.Recovery()
	for {
		select {
		case <-this.exit:
			logger.I("Restore", "EXIT restore")
			this.waiter.Done()
			return
		default:
			if len(this.output) > this.limit {
				this.updateStoreStatus(true)
				time.Sleep(time.Millisecond * 100)
			} else if data, err := this.queue.Pop(); err != nil {
				if err == fqueue.QueueEmpty || err.Error() == meta.RedisNilError || err == goque.ErrEmpty {
					this.updateStoreStatus(false)
					time.Sleep(100 * time.Millisecond)
				} else {
					logger.E("Restore", "%v QueuePopFailed,err%v,data:%v", this.queue.GetStorerType(), err, data)
					time.Sleep(100 * time.Millisecond)
				}
			} else {
				if len(data) == 0 {
					this.updateStoreStatus(false)
					time.Sleep(100 * time.Millisecond)
				} else {
					this.updateStoreStatus(true)
					logger.I("Restore", "%v QueueRESTORE %s", this.queue.GetStorerType(), string(data))
					this.output <- data
				}
			}
		}
	}
}

func (this *PChan) monitor() {
	defer meta.Recovery()
	t := time.NewTicker(time.Millisecond * 100)
	tt := time.NewTicker(time.Second)
	ttt := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-this.exit:
			t.Stop()
			tt.Stop()
			ttt.Stop()
			logger.I("Monitor", "exit")
			this.waiter.Done()
			return
		case <-t.C:
			if len(this.output) >= this.limit {
				this.updateStoreStatus(true)
			}
		case <-tt.C:
			if len(this.output) >= this.limit/2 || len(this.input) > this.limit/2 {
				logger.W("BufferWarn", "input:%d,output:%d", len(this.input), len(this.output))
			}
		case <-ttt.C:
			logger.I("BufferLen", "input:%d,output:%d", len(this.input), len(this.output))
		}
	}
}

func (this *PChan) Close() {
	close(this.exit)
	this.waiter.Wait()

	if len(this.output) > 0 {
		logger.W("FlushToQueue", "OutputLen:%d\n", len(this.output))
	}
	this.storeAndCloseChan(this.output)

	if len(this.input) > 0 {
		logger.W("FlushToQueue", "InputLen:%d\n", len(this.input))
	}
	this.storeAndCloseChan(this.input)
	if this.queue.GetWriterOffset() != this.queue.GetReaderOffset() {
		logger.W("FlushToQueue", "msg:%d in disk", this.queue.GetWriterOffset()-this.queue.GetReaderOffset())
	}
	logger.I("FlushToQueue", "InputEnd")
}

func (this *PChan) storeAndCloseChan(channel chan []byte) {
	close(channel)
	for {
		msg, more := <-channel
		if more {
			err := this.queue.Push([]byte(msg))
			if err != nil {
				logger.E("storeAndCloseChan", "%v QueuePushFailed,err:%v,msg:%v", this.queue.GetStorerType(), err, msg)
			} else {
				logger.I("storeAndCloseChan", "%v QueuePushSuccess,msg:%v", this.queue.GetStorerType(), msg)
			}
		} else {
			break
		}
	}
}
