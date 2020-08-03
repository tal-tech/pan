package kafka

import (
	"bytes"
	"sync/atomic"
	"time"

	"pan/failmode"
	"pan/internal"
	"pan/meta"
	"pan/mq"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"
)

type Kafka struct {
	input    chan []byte
	fallback chan<- []byte

	successCount int64
	errorCount   int64

	cfg      *sarama.Config
	producer sarama.AsyncProducer
	failMode failmode.FailMode

	exit        chan struct{}
	watchExit   chan struct{}
	successExit chan struct{}
	fallExit    chan struct{}
}

func init() {
	kafkaEnable := cast.ToBool(confutil.GetConfDefault("KafkaProxy", "enable", "false"))
	if kafkaEnable {
		mq.AddMQ(meta.Kafka, Init)
	}
}

func Init(quit chan struct{}, fallBack chan<- []byte) mq.MQ {
	cfg := sarama.NewConfig()
	partitioner := confutil.GetConfDefault("KafkaProxy", "KafkaPartitioner", "round")
	cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	if partitioner == "hash" {
		cfg.Producer.Partitioner = sarama.NewHashPartitioner
	}
	if partitioner == "random" {
		cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	}
	cfg.Producer.Timeout = cast.ToDuration(confutil.GetConfDefault("KafkaProxy", "KafkaProducerTimeout", "10")) * time.Second
	cfg.Producer.Flush.MaxMessages = 5000
	waitall := confutil.GetConf("KafkaProxy", "KafkaWaitAll")
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	if confutil.GetConfDefault("KafkaProxy", "sasl", "false") == "true" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = confutil.GetConf("KafkaProxy", "user")
		cfg.Net.SASL.Password = confutil.GetConf("KafkaProxy", "password")
	}
	if waitall != "" {
		cfg.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		cfg.Producer.RequiredAcks = sarama.WaitForLocal
	}
	compression := confutil.GetConf("KafkaProxy", "KafkaCompression")
	if compression != "" {
		cfg.Producer.Compression = sarama.CompressionSnappy
	}

	k := new(Kafka)
	k.cfg = cfg
	k.input = make(chan []byte, 0)
	k.fallback = fallBack
	k.exit = make(chan struct{}, 1)
	k.watchExit = make(chan struct{}, 1)
	k.successExit = make(chan struct{}, 1)
	k.fallExit = make(chan struct{}, 1)
	k.successCount = int64(0)
	k.errorCount = int64(0)

	k.initProducer()
	go k.success()
	go k.fall()
	go k.run(quit)
	return k
}

func (k *Kafka) Input() chan<- []byte {
	return k.input
}

func (k *Kafka) Close() {
	close(k.exit)
}

func (k *Kafka) SetFailMode() {
	var err error
	k.failMode, err = failmode.GetFailMode(meta.Kafka, confutil.GetConfDefault("KafkaProxy", "failMode", "retry"))
	if err != nil {
		logger.F("Kafka SetFailMode error", err)
	}
}

func (k *Kafka) initProducer() {
	hosts := confutil.GetConfs("KafkaProxy", "brokers")
	producer, err := sarama.NewAsyncProducer(hosts, k.cfg)
	if err != nil {
		logger.F("ProducerInit", err)
	}
	k.producer = producer
}

func (k *Kafka) success() {
	defer meta.Recovery()
	for {
		select {
		case <-k.successExit:
			return
		case msg := <-k.producer.Successes():
			if msg != nil {
				atomic.AddInt64(&k.successCount, int64(1))

				byteMsg := msg.Metadata.([]byte)
				byteMsg = byteMsg[:0]
				internal.BytePool.Put(byteMsg)
			}
		}
	}
}

func (k *Kafka) fall() {
	defer meta.Recovery()
	for {
		select {
		case <-k.fallExit:
			return
		case err := <-k.producer.Errors():
			if err != nil {
				atomic.AddInt64(&k.errorCount, int64(1))
				items := bytes.SplitN(err.Msg.Metadata.([]byte), []byte(" "), 5)
				timestamp := string(items[0])
				loggerid, _ := err.Msg.Key.Encode()
				data, _ := err.Msg.Value.Encode()
				logFlag := cast.ToInt64(timestamp) % 10
				diff := time.Now().Unix() - cast.ToInt64(timestamp)/10
				if logFlag == 0 { //首次错误
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加1秒
					logger.E("ProducerOutPut", "Kafka Send Message Error:'%v',Topic:'%s',Data:'%v'", err.Err, err.Msg.Topic, err.Msg.Value)
				}
				if diff >= 59 { //
					timestamp = cast.ToString(time.Now().Unix()*10 + 11) //多加一秒，防止同一秒可能出现打多次
					logger.E("ProducerOutPut", "Kafka Send Message Error:'%v',Topic:'%s',Data:'%v'", err.Err, err.Msg.Topic, err.Msg.Value)
				}
				k.failMode.Do(k.fallback, []byte(timestamp+" "+meta.Kafka+" "+err.Msg.Topic+" "+string(loggerid)+" "+string(data)), data, []interface{}{meta.Kafka, err.Msg.Topic})
				byteMsg := err.Msg.Metadata.([]byte)
				byteMsg = byteMsg[:0]
				internal.BytePool.Put(byteMsg)
			}
		}
	}
}

func (k *Kafka) run(quite chan struct{}) {
	defer meta.Recovery()
	t := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-t.C:
			succ := atomic.SwapInt64(&k.successCount, 0)
			fail := atomic.SwapInt64(&k.errorCount, 0)
			logger.I("KafkaProducerOutPut", "Stat succ:%d,fail:%d", succ, fail)
		case msg := <-k.input:
			items := bytes.SplitN(msg, []byte(" "), 5)
			topic := string(items[2])
			loggerid := items[3]
			data := items[4]
			k.producer.Input() <- &sarama.ProducerMessage{
				Metadata: msg,
				Topic:    topic,
				Key:      sarama.ByteEncoder(loggerid),
				Value:    sarama.ByteEncoder(data),
			}
		case <-k.exit:
			t.Stop()
			logger.I("ProducerOutPut", "KafkaQuit begin")
			if errors, ok := k.producer.Close().(sarama.ProducerErrors); ok {
				for _, err := range errors {
					items := bytes.SplitN(err.Msg.Metadata.([]byte), []byte(" "), 5)
					timestamp := string(items[0])
					loggerid, _ := err.Msg.Key.Encode()
					data, _ := err.Msg.Value.Encode()
					k.fallback <- []byte(timestamp + " " + meta.Kafka + " " + err.Msg.Topic + " " + string(loggerid) + " " + string(data))
				}
				logger.I("ProducerOutPut", "KafkaQuit recovered:%d", len(errors))
			}
			logger.I("ProducerOutPut", "KafkaQuit end")
			close(k.watchExit)
			close(k.successExit)
			close(k.fallExit)
			close(quite)
			return
		}
	}
}
