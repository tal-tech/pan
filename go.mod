module pan

go 1.13

require (
	github.com/Joinhack/fqueue v0.0.0-20140801051330-9afb37fd0de2
	github.com/Shopify/sarama v1.24.1
	github.com/Unknwon/goconfig v0.0.0-20191126170842-860a72fb44fd // indirect
	github.com/apache/rocketmq-client-go v0.0.0
	github.com/beeker1121/goque v2.1.0+incompatible
	github.com/emirpasic/gods v1.12.0 // indirect
	github.com/facebookgo/ensure v0.0.0-20160127193407-b4ab57deab51 // indirect
	github.com/facebookgo/freeport v0.0.0-20150612182905-d4adf43b75b9 // indirect
	github.com/facebookgo/grace v0.0.0-20180706040059-75cf19382434
	github.com/facebookgo/stack v0.0.0-20160209184415-751773369052 // indirect
	github.com/facebookgo/subset v0.0.0-20150612182917-8dac2c3c4870 // indirect
	github.com/gin-gonic/gin v1.6.3 // indirect
	github.com/go-redis/redis v6.15.8+incompatible // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/mock v1.4.3 // indirect
	github.com/henrylee2cn/goutil v0.0.0-20200528092701-9d8a093584ee // indirect
	github.com/henrylee2cn/teleport v5.0.0+incompatible
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/nsqio/go-nsq v1.0.7
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/smallnest/rpcx v0.0.0-20200522154920-57cd85a3467c // indirect
	github.com/spf13/cast v1.3.1
	github.com/streadway/amqp v0.0.0-20190827072141-edfb9018d271
	github.com/tal-tech/connPool v0.0.0-20200806112113-738c408fe6ae // indirect
	github.com/tal-tech/loggerX v0.0.0-20200806121626-bc3db51db258
	github.com/tal-tech/routinePool v0.0.0-20200806121001-477db7bdba8a // indirect
	github.com/tal-tech/xredis v0.0.0-20200806132427-7807ee6297d9
	github.com/tal-tech/xtools v0.0.0-20200806122720-d016f6d90e97
	github.com/tidwall/gjson v1.6.0 // indirect
	go.uber.org/zap v1.15.0 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
)

replace github.com/apache/rocketmq-client-go v0.0.0 => github.com/hhtlxhhxy/github.com_apache_rocketmq-client-go-producer v1.0.0

replace github.com/henrylee2cn/teleport v5.0.0+incompatible => github.com/hhtlxhhxy/github.com_henrylee2cn_teleport v1.0.0
