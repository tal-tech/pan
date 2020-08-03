package store

import (
	"context"

	"pan/meta"

	"git.100tal.com/wangxiao_go_lib/redisdao"
)

type Redis struct {
	writerOffset int
	readerOffset int
	stype        string
}

func init() {
	if stype == meta.REDIS {
		AddStorer(stype, InitRedis)
	}
}

func InitRedis() (Storer, error) {
	this := new(Redis)
	this.stype = meta.REDIS
	return this, nil
}

func (this *Redis) Push(msg []byte) error {
	data := []interface{}{string(msg)}
	redis := redisdao.NewSimpleXesRedis(context.Background(), "redis")
	_, err := redis.LPush("pan_queue", nil, data)
	this.writerOffset += int(len(msg) + 2)
	return err
}

func (this *Redis) Pop() ([]byte, error) {
	redis := redisdao.NewSimpleXesRedis(context.Background(), "redis")
	data, err := redis.RPop("pan_queue", nil)
	ret := []byte(data)
	this.readerOffset += int(2 + len(ret))
	return ret, err
}

func (this *Redis) GetWriterOffset() int {
	return this.writerOffset
}

func (this *Redis) GetReaderOffset() int {
	return this.readerOffset
}

func (this *Redis) GetStorerType() string {
	return this.stype
}
