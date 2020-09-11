# <img src="https://github.com/hhtlxhhxy/pan/blob/master/img/pan.jpg" alt="image-20200803155136931" style="zoom:50%;" />

-----
## Background
-----
Pan is a high performance and stable production side agent of messager-oriented middleware written in pure Go language. It supports mainstream message queues in the market, such as Kafka, RabbitMQ, RocketMQ, NSQ, etc. Moreover, it is easy to be extended and can meet different business requirements in the production environment.


## Document
-----
[Document](https://tal-tech.github.io/pan-doc/)

## Framework
------
The framework of Pan is shown as below.

<img src="https://github.com/hhtlxhhxy/pan/blob/master/img/fram1.jpg" alt="image-20200803155136931" style="zoom:50%;" />

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
enable=true
KafkaWaitAll=true
KafkaCompression=true
KafkaPartitioner=round
KafkaProducerTimeout=10
brokers=localhost:9092
sasl=false
user=
password=
valid= //topic whitelist，if empty, all topic can be sended
failMode=retry/save/discard

```

#### 5. Run
```shell
tar -zxvf pan.tar.gz
cd pan/
make
./bin/pan -c ../conf/conf.ini
```

#### 6. Send Message

```go
package main
 
import (
    "fmt"
    "time"
 
    "github.com/tal-tech/xtools/kafkautil"

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
#### modify conf
```shell
[KafkaProxy]
unix=/home/www/pan/pan.sock   //sock in pan
host=localhost:9999  //ip and post pan listen
```

#### warn
replace in go.mod
```shell
replace github.com/henrylee2cn/teleport v5.0.0+incompatible => github.com/hhtlxhhxy/github.com_henrylee2cn_teleport v1.0.0

或

replace github.com/henrylee2cn/teleport v0.0.0 => github.com/hhtlxhhxy/github.com_henrylee2cn_teleport v1.0.0
```
