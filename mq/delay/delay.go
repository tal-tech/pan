/**
 *Created by He Haitao at 2020/4/9 11:26 上午
 */
package delay

import (
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"time"

	"pan/failmode"
	"pan/meta"
	"pan/mq"

	"git.100tal.com/wangxiao_go_lib/redisdao"
	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"
)

type Delay struct {
	input    chan []byte
	fallback chan<- []byte
	failMode failmode.FailMode

	successCount int64
	errorCount   int64

	cfg  *sarama.Config
	exit chan struct{}
}

func init() {
	delayEnable := cast.ToBool(confutil.GetConfDefault("DelayMQProxy", "enable", "false"))
	if delayEnable {
		mq.AddMQ(meta.Delay, Init)
	}
}

func Init(quit chan struct{}, fallBack chan<- []byte) mq.MQ {
	d := new(Delay)
	d.input = make(chan []byte, 0)
	d.fallback = fallBack
	d.exit = make(chan struct{}, 1)
	d.successCount = int64(0)
	d.errorCount = int64(0)
	go d.dispatch()
	go d.run(quit)
	return d
}

func (d *Delay) Input() chan<- []byte {
	return d.input
}

func (d *Delay) Close() {
	close(d.exit)
}

func (d *Delay) SetFailMode() {
	var err error
	d.failMode, err = failmode.GetFailMode(meta.Delay, confutil.GetConfDefault("DelayMQProxy", "failMode", "retry"))
	if err != nil {
		logger.F("DelayMQ SetFailMode error", err)
	}
}

func (d *Delay) dispatch() {
	t := time.NewTicker(time.Second * 1)
	keyName := confutil.GetConfDefault("DelayMQProxy", "key", "delay_queue")
	limit := cast.ToInt(confutil.GetConfDefault("DelayMQProxy", "limit", "1000"))
	for {
		select {
		case <-t.C:
			now := time.Now().Unix()
			redis := redisdao.NewSimpleXesRedis(context.Background(), "delayRedis")
			backs, err := redis.ZRangeByScore(keyName, nil, 0, now, "limit", 0, limit)
			if err != nil {
				logger.E("DelayMQPRoxy", "message read from redis error[%v]", err)
				return
			}
			if len(backs) > 0 {
				_, err = redis.ZRemRangeByRank(keyName, nil, 0, len(backs)-1)
				if err != nil {
					logger.E("DelayMQPRoxy", "delete message from redis error[%v]", err)
				}
			}

			for _, v := range backs {
				items := bytes.SplitN([]byte(v), []byte(" "), 4)
				mqType := string(items[2])
				switch mqType {
				case meta.Kafka:
					var sendTimestamp int64
					kafkaItems := bytes.SplitN([]byte(v), []byte(" "), 7)
					sendTimestamp = cast.ToInt64(string(kafkaItems[5]))
					if int64(math.Abs(float64(sendTimestamp-now))) <= 5 {
						d.fallback <- []byte(string(kafkaItems[0]) + " " + meta.Kafka + " " + string(kafkaItems[3]) + " " + string(kafkaItems[4]) + " " + string(kafkaItems[6]))
					}
				}
			}
		case <-d.exit:
			t.Stop()
			logger.I("DelayMQRProxy", "DelayMQ dispatch exit")
			return
		}
	}
}
func (d *Delay) run(quite chan struct{}) {
	defer meta.Recovery()
	t := time.NewTicker(time.Second * 60)
	keyName := confutil.GetConfDefault("DelayMQProxy", "key", "delay_queue")
	for {
		select {
		case <-t.C:
			success := atomic.SwapInt64(&d.successCount, 0)
			fail := atomic.SwapInt64(&d.errorCount, 0)
			logger.I("DelayMQOutPut", "Stat success:%d,fail:%d", success, fail)
		case msg := <-d.input:
			items := bytes.Split(msg, []byte(" "))
			if len(items) > 3 {
				mqType := string(items[2])
				switch mqType {
				case meta.Kafka:
					var sendTimestamp int64
					if len(items) > 6 {
						sendTimestamp = cast.ToInt64(string(items[5]))
					}
					if time.Now().Unix() > sendTimestamp {
						logger.E("DelayMQPRoxy", "message expired, message:%v", msg)
						atomic.AddInt64(&d.errorCount, int64(1))
					} else {
						redis := redisdao.NewSimpleXesRedis(context.Background(), "delayRedis")
						_, err := redis.ZAdd(keyName, nil, []interface{}{sendTimestamp, string(msg)})
						if err != nil {
							logger.E("DelayMQPRoxy", "message add to redis error[%v] message:%v", err, msg)
							atomic.AddInt64(&d.errorCount, int64(1))
						}
						atomic.AddInt64(&d.successCount, int64(1))
					}
				}
			}
		case <-d.exit:
			t.Stop()
			logger.I("DelayMQRProxy", "DelayMQ exit end")
			close(quite)
			return
		}
	}
}
