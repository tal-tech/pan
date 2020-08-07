/**
 *Created by He Haitao at 2019/11/6 3:57 下午
 */
package nsq

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"pan/failmode"
	"pan/internal"
	"pan/meta"
	"pan/mq"

	gonsq "github.com/nsqio/go-nsq"
	"github.com/spf13/cast"
	logger "github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
)

type NSQ struct {
	input    chan []byte
	fallback chan<- []byte

	successCount int64
	errorCount   int64
	failMode     failmode.FailMode

	producer     *gonsq.Producer
	responseChan chan *gonsq.ProducerTransaction
	lock         *sync.RWMutex

	exit chan struct{}
}

func init() {
	nsqEnable := cast.ToBool(confutil.GetConfDefault("NSQProxy", "enable", "false"))
	if nsqEnable {
		mq.AddMQ(meta.NSQ, Init)
	}
}

func Init(quit chan struct{}, fallBack chan<- []byte) mq.MQ {
	n := new(NSQ)
	n.exit = make(chan struct{}, 1)
	n.fallback = fallBack
	n.responseChan = make(chan *gonsq.ProducerTransaction, 0)
	n.input = make(chan []byte, 0)
	n.lock = new(sync.RWMutex)
	n.successCount = int64(0)
	n.errorCount = int64(0)

	n.initProducer()
	go n.response()
	go n.run(quit)
	return n
}

func (n *NSQ) Input() chan<- []byte {
	return n.input
}

func (n *NSQ) Close() {
	close(n.exit)
}

func (n *NSQ) SetFailMode() {
	var err error
	n.failMode, err = failmode.GetFailMode(meta.NSQ, confutil.GetConfDefault("NSQProxy", "failMode", "retry"))
	if err != nil {
		logger.F("NSQ SetFailMode error", err)
	}
}

func (n *NSQ) initProducer() {
	config := gonsq.NewConfig()
	p, err := gonsq.NewProducer(confutil.GetConf("NSQProxy", "nsqdAddr"), config)
	if err != nil {
		logger.F("NSQInitProducer", " init nsq producer error :%v", err)
	}
	n.producer = p
}

func (n *NSQ) response() {
	defer meta.Recovery()
	for {
		select {
		case trans := <-n.responseChan:
			msg := trans.Args[0].([]byte)
			items := bytes.SplitN(msg, []byte(" "), 5)
			topic := string(items[2])
			key := string(items[3])
			timestamp := string(items[0])
			data := items[4]

			if trans.Error != nil {
				atomic.AddInt64(&n.errorCount, int64(1))
				logFlag := cast.ToInt64(timestamp) % 10
				diff := time.Now().Unix() - cast.ToInt64(timestamp)/10
				if logFlag == 0 { //首次错误
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加1秒
					logger.E("ProducerOutPut", "NSQ Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", trans.Error, topic, key, data)
				}
				if diff >= 59 {
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加一秒，防止同一秒可能出现打多次
					logger.E("ProducerOutPut", "NSQ Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", trans.Error, topic, key, data)
				}
				n.failMode.Do(n.fallback, []byte(timestamp+" "+meta.NSQ+" "+topic+" "+key+" "+string(data)), data, []interface{}{meta.NSQ, topic + "_" + key})
				msg = msg[:0]
				internal.BytePool.Put(msg)
			} else {
				atomic.AddInt64(&n.successCount, int64(1))
				msg = msg[:0]
				internal.BytePool.Put(msg)
			}
		}
	}
}

func (n *NSQ) run(quit chan struct{}) {
	defer meta.Recovery()
	t := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-t.C:
			succ := atomic.SwapInt64(&n.successCount, 0)
			fail := atomic.SwapInt64(&n.errorCount, 0)
			logger.I("NSQProducerOutput", "Stat succ:%d,fail:%d", succ, fail)
		case msg := <-n.input:
			items := bytes.SplitN(msg, []byte(" "), 5)
			topic := string(items[2])
			key := string(items[3])
			timestamp := string(items[0])
			data := items[4]

			err := n.producer.PublishAsync(topic, data, n.responseChan, msg)
			if err != nil {
				atomic.AddInt64(&n.errorCount, int64(1))
				logFlag := cast.ToInt64(timestamp) % 10
				diff := time.Now().Unix() - cast.ToInt64(timestamp)/10
				if logFlag == 0 { //首次错误
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加1秒
					logger.E("ProducerOutPut", "NSQ Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", err, topic, key, data)
				}
				if diff >= 59 {
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加一秒，防止同一秒可能出现打多次
					logger.E("ProducerOutPut", "NSQ Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", err, topic, key, data)
				}
				n.failMode.Do(n.fallback, []byte(timestamp+" "+meta.NSQ+" "+topic+" "+key+" "+string(data)), data, []interface{}{meta.NSQ, topic + "_" + key})
				msg = msg[:0]
				internal.BytePool.Put(msg)
			}
		case <-n.exit:
			t.Stop()
			logger.I("NSQOutput", "NSQQuit OK")
			close(quit)
			return
		}
	}
}
