package main

import (
	"testing"

	"github.com/tal-tech/xtools/kafkautil"
)

func TestSendMessageToKafka(t *testing.T) {
	message := []byte("hello world!")
	err := kafkautil.Send2Proxy("test", message)
	if err != nil {
		t.Errorf("error messgae:%+v", err)
	} else {
		t.Logf("send message to kafka success:%+v", string(message))
	}
}
