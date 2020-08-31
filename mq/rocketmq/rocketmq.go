/**
 *Created by He Haitao at 2019/10/31 7:06 下午
 */
package rocketmq

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pan/failmode"
	"pan/internal"
	"pan/meta"
	"pan/mq"

	"github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/producer"
	"github.com/spf13/cast"
	logger "github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
)

type Rocketmq struct {
	input    chan []byte
	fallback chan<- []byte

	successCount int64
	errorCount   int64
	failMode     failmode.FailMode

	producer rocketmq.Producer
	lock     *sync.RWMutex

	exit chan struct{}
}

func init() {
	rocketmqEnable := cast.ToBool(confutil.GetConfDefault("RocketmqProxy", "enable", "false"))
	if rocketmqEnable {
		mq.AddMQ(meta.Rocketmq, Init)
	}
}

func Init(quit chan struct{}, fallBack chan<- []byte) mq.MQ {
	r := new(Rocketmq)
	r.exit = make(chan struct{}, 1)
	r.fallback = fallBack
	r.input = make(chan []byte, 0)
	r.lock = new(sync.RWMutex)
	r.successCount = int64(0)
	r.errorCount = int64(0)

	r.initProducer()
	go r.run(quit)
	return r
}

func (r *Rocketmq) Input() chan<- []byte {
	return r.input
}

func (r *Rocketmq) Close() {
	close(r.exit)
}

func (r *Rocketmq) SetFailMode() {
	var err error
	r.failMode, err = failmode.GetFailMode(meta.Rocketmq, confutil.GetConfDefault("RocketmqProxy", "failMode", "discard"))
	if err != nil {
		logger.F("Rocketmq SetFailMode error", err)
	}
}

func (r *Rocketmq) initProducer() {
	p, err := rocketmq.NewProducer(
		producer.WithGroupName(confutil.GetConf("RocketmqProxy", "producerGroup")),
		producer.WithNameServer(strings.Split(confutil.GetConf("RocketmqProxy", "nameServer"), ",")),
		producer.WithRetry(cast.ToInt(confutil.GetConf("RocketmqProxy", "producerRetry"))),
		producer.WithQueueSelector(producer.NewManualQueueSelector()),
	)
	if err != nil {
		logger.F("RocketmqNewProducer", "Rocketmq NewProducer error:%v", err)
	}
	r.producer = p
	err = p.Start()
	if err != nil {

		logger.F("RocketmqNewProducer", "Rocketmq NewProducer start error:%v", err)
	}
}

func (r *Rocketmq) run(quit chan struct{}) {
	defer meta.Recovery()
	t := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-t.C:
			succ := atomic.SwapInt64(&r.successCount, 0)
			fail := atomic.SwapInt64(&r.errorCount, 0)
			logger.I("RocketmqProducerOutput", "Stat succ:%d,fail:%d", succ, fail)
		case msg := <-r.input:
			items := bytes.SplitN(msg, []byte(" "), 6)
			timestamp := string(items[0])
			topic := string(items[2])
			tag := string(items[3])
			key := string(items[4])
			data := items[5]

			mess := primitive.NewMessage(topic, data)

			mess.PutProperty(primitive.PropertyTags, tag)
			mess.PutProperty(primitive.PropertyKeys, key)
			err := r.producer.SendAsync(context.Background(),
				func(ctx context.Context, result *primitive.SendResult, e error) {
					if e != nil {
						atomic.AddInt64(&r.errorCount, int64(1))

						logFlag := cast.ToInt64(timestamp) % 10
						diff := time.Now().Unix() - cast.ToInt64(timestamp)/10
						if logFlag == 0 { //首次错误
							timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加1秒
							logger.E("ProducerOutPut", "Rocketmq Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", e, topic, tag, key, data)
						}
						if diff >= 59 {
							timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加一秒，防止同一秒可能出现打多次
							logger.E("ProducerOutPut", "Rocketmq Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", e, topic, tag, key, data)
						}

						r.failMode.Do(r.fallback, []byte(timestamp+" "+meta.Rocketmq+" "+topic+" "+tag+" "+key+" "+string(data)), data, []interface{}{meta.Rocketmq, topic + "_" + tag + "_" + key})
						msg = msg[:0]
						internal.BytePool.Put(msg)
					} else {
						atomic.AddInt64(&r.successCount, int64(1))

						msg = msg[:0]
						internal.BytePool.Put(msg)
					}
				}, mess)
			if err != nil {
				atomic.AddInt64(&r.errorCount, int64(1))
				logFlag := cast.ToInt64(timestamp) % 10
				diff := time.Now().Unix() - cast.ToInt64(timestamp)/10
				if logFlag == 0 { //首次错误
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加1秒
					logger.E("ProducerOutPut", "Rocketmq Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", err, topic, tag, key, data)
				}
				if diff >= 59 {
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加一秒，防止同一秒可能出现打多次
					logger.E("ProducerOutPut", "Rocketmq Send Message Error:'%v',Topic:'%s',Tag:'%s',Key:'%s',Data:'%v'", err, topic, tag, key, data)
				}
				r.failMode.Do(r.fallback, []byte(timestamp+" "+meta.Rocketmq+" "+topic+" "+tag+" "+key+" "+string(data)), data, []interface{}{meta.Rocketmq, topic + "_" + tag + "_" + key})
				msg = msg[:0]
				internal.BytePool.Put(msg)
			}
		case <-r.exit:
			t.Stop()
			r.producer.Shutdown()
			logger.I("RocketmqOutput", "RocketmqQuit OK")
			close(quit)
			return
		}
	}
}
