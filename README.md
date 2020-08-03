# Pan
-----

## Background
-----
Pan is a high performance and stable production side agent of messager-oriented middleware written in pure Go language. It supports mainstream message queues in the market, such as Kafka, RabbitMQ, RocketMQ, NSQ, etc. Moreover, it is easy to be extended and can meet different business requirements in the production environment.

## Framework
------
The framework of Pan is shown as below.

<img src="https://github.com/hhtlxhhxy/pan/blob/master/img/framework.jpg" alt="image-20200803155136931" style="zoom:50%;" />

## Quickstart

### Produce messages to kafka by Pan.
-----

#### 1. Start zookeeper
```shell
./bin/zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
```
#### 2. Start kafka
```shell
./bin/kafka-server-start /usr/local/etc/kafka/server.properties
```
#### 3. Create topic
```shell
./bin/kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
```
#### 4. Modify config for kafka in Pan
```shell
[KafkaProxy]
enable=true//开关
KafkaWaitAll=true
KafkaCompression=true
KafkaPartitioner=round
KafkaProducerTimeout=10
brokers=localhost:9092
sasl=false
user=
password=
valid= //topic白名单，若为空则所有topic均可发送
failMode=retry/save/discard(无限次重试、保存到redis、丢弃)

```
#### 5. Make
```shell
make
```
#### 6. Run
```shell
./bin/triton -c ../conf/conf.ini
```

#### 7. Send Message

```go
package main
 
import (
    "fmt"
    "time"
 
    "git.100tal.com/wangxiao_go_lib/xesTools/kafkautil"

    "github.com/spf13/cast"
)
 
func main() {
    t := time.Tick(5 * time.Second)
    count := 0
    for {
        select {
        case <-t:
            count++
            err := kafkautil.Send2Proxy("test", []byte("kafka "+cast.ToString(count)))
            if err != nil {
                fmt.Println(err)
            }
            continue
        }
    }
}
```
#### 使用配置
```shell
[KafkaProxy]
unix=/home/www/pan/pan.sock   //pan的sock文件地址
host=localhost:9999  //pan的ip:port地址
```

#### 注意事项
注意go.mod文件中替换包
```shell
replace github.com/henrylee2cn/teleport v5.0.0+incompatible => git.100tal.com/wangxiao_xueyan_gomirrors/github.com_henrylee2cn_teleport v1.0.0

或

replace github.com/henrylee2cn/teleport v0.0.0 => git.100tal.com/wangxiao_xueyan_gomirrors/github.com_henrylee2cn_teleport v1.0.0
```
