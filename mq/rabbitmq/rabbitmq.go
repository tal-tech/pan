/**
 *Created by He Haitao at 2019/10/25 4:39 下午
 */
package rabbitmq

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"pan/failmode"
	"pan/internal"
	"pan/meta"
	"pan/mq"

	"github.com/spf13/cast"
	"github.com/streadway/amqp"
	logger "github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
)

type Rabbitmq struct {
	input    chan []byte
	fallback chan<- []byte

	successCount int64
	errorCount   int64
	fallbackFlag bool
	failMode     failmode.FailMode

	url     string
	channel *amqp.Channel
	conn    *amqp.Connection
	lock    *sync.RWMutex

	exit chan struct{}
}

func init() {
	rabbitmqEnable := cast.ToBool(confutil.GetConfDefault("RabbitmqProxy", "enable", "false"))
	if rabbitmqEnable {
		mq.AddMQ(meta.Rabbitmq, Init)
	}
}

func Init(quit chan struct{}, fallBack chan<- []byte) mq.MQ {
	r := new(Rabbitmq)
	r.exit = make(chan struct{}, 1)
	r.fallback = fallBack
	r.input = make(chan []byte, 0)
	r.url = confutil.GetConf("RabbitmqProxy", "url")
	r.lock = new(sync.RWMutex)
	r.successCount = int64(0)
	r.errorCount = int64(0)
	r.fallbackFlag = cast.ToBool(confutil.GetConfDefault("RabbitmqProxy", "fallback", "true"))

	r.initChannel()

	go r.run(quit)
	return r
}

func (r *Rabbitmq) Input() chan<- []byte {
	return r.input
}

func (r *Rabbitmq) Close() {
	close(r.exit)
}

func (r *Rabbitmq) SetFailMode() {
	var err error
	r.failMode, err = failmode.GetFailMode(meta.Rabbitmq, confutil.GetConfDefault("RabbitmqProxy", "failMode", "retry"))
	if err != nil {
		logger.F("Rabbitmq SetFailMode error", err)
	}
}

func (r *Rabbitmq) initChannel() {
	var err error
	conn, err := amqp.Dial(r.url)
	if err != nil {
		logger.E("RabbitmqChannel", "Rabbitmq channel init error :%v", err)
		return
	}
	r.conn = conn
	r.channel, err = conn.Channel()
	if err != nil {
		logger.E("RabbitmqChannel", "Rabbitmq channel init error :%v", err)
		return
	}
}

func (r *Rabbitmq) run(quit chan struct{}) {
	defer meta.Recovery()
	t := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-t.C:
			succ := atomic.SwapInt64(&r.successCount, 0)
			fail := atomic.SwapInt64(&r.errorCount, 0)
			logger.I("RabbitmqProducerOutput", "Stat succ:%d,fail:%d", succ, fail)
		case msg := <-r.input:
			items := bytes.SplitN(msg, []byte(" "), 6)
			exchangeName := string(items[2])
			routingKey := string(items[3])
			expire := string(items[4])
			timestamp := string(items[0])
			data := items[5]
			if expire == "-1" {
				expire = ""
			}

			err := r.channel.Publish(
				exchangeName, // exchange
				routingKey,   // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Expiration:  expire,
					Body:        data,
				})
			if err != nil {
				if r.conn.IsClosed() {
					r.initChannel()
				}
				atomic.AddInt64(&r.errorCount, int64(1))
				logFlag := cast.ToInt64(timestamp) % 10
				diff := time.Now().Unix() - cast.ToInt64(timestamp)/10
				if logFlag == 0 { //首次错误
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加1秒
					logger.E("ProducerOutPut", "Rabbitmq Send Message Error:'%v',Exchange:'%s',RoutingKey:'%s',Data:'%v'", err, exchangeName, routingKey, data)
				}
				if diff >= 59 {
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加一秒，防止同一秒可能出现打多次
					logger.E("ProducerOutPut", "Rabbitmq Send Message Error:'%v',Exchange:'%s',RoutingKey:'%s',Data:'%v'", err, exchangeName, routingKey, data)
				}
				r.failMode.Do(r.fallback, []byte(timestamp+" "+meta.Rabbitmq+" "+exchangeName+" "+routingKey+" "+string(data)+" "+expire), data, []interface{}{meta.Rabbitmq, exchangeName + "_" + routingKey})
				msg = msg[:0]
				internal.BytePool.Put(msg)
			} else {
				atomic.AddInt64(&r.successCount, int64(1))
				msg = msg[:0]
				internal.BytePool.Put(msg)
			}
		case <-r.exit:
			t.Stop()
			logger.I("RabbitmqOutPut", "RabbitmqQuit OK")
			close(quit)
			return
		}
	}
}
