/**
 *Created by He Haitao at 2019/11/26 3:06 下午
 */
package mq

import (
	"bytes"

	"pan/internal"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"

	"github.com/spf13/cast"
)

type Init func(exit chan struct{}, fallBack chan<- []byte) MQ

var (
	initMap = make(map[string]Init, 0)
)

type MQ interface {
	Input() chan<- []byte
	Close()
	SetFailMode()
}

func AddMQ(mqType string, init Init) {
	initMap[mqType] = init
}

type ProxyManager struct {
	pchan  *internal.PChan
	server *internal.Server

	input    <-chan []byte
	fallback chan<- []byte

	mqMap map[string]MQ
	quits []chan struct{}
}

func NewProxyManager() (*ProxyManager, error) {
	pm := new(ProxyManager)
	confs := confutil.GetConfStringMap("Pan")
	bufferLimit := cast.ToInt64(confutil.GetConfDefault("Pan", "bufferLimit", "2000"))
	var err error
	pm.pchan, err = internal.NewPChan(bufferLimit)
	if err != nil {
		return nil, err
	}
	ma := make([]string, 0)
	for key, _ := range initMap {
		ma = append(ma, key)
	}
	pm.server = internal.NewServer(confs["tcpPort"], confs["unixSocket"], ma, pm.pchan.Input)

	pm.input = pm.pchan.Output
	pm.fallback = pm.pchan.Input
	pm.mqMap = make(map[string]MQ, 0)

	pm.quits = make([]chan struct{}, 0)
	for mqType, init := range initMap {
		quit := make(chan struct{}, 1)
		mq := init(quit, pm.fallback)
		mq.SetFailMode()
		pm.mqMap[mqType] = mq
		pm.quits = append(pm.quits, quit)
	}

	return pm, nil
}

func (m *ProxyManager) Run(exit <-chan string) {
	for {
		select {
		case msg := <-m.input:
			items := bytes.Split(msg, []byte(" "))
			mqType := ""
			if len(items) > 2 {
				mqType = string(items[1])
			}
			mq := m.mqMap[mqType] //判断消息类型，并将其发送到对应的mq
			if mq != nil {
				mq.Input() <- msg
			} else {
				logger.E("ProxyManager", "not support mq type%s", string(mqType))
			}
		case <-exit:
			logger.I("ProxyManager", "server will close")
			m.server.Close()
			logger.I("ProxyManager", "server closed")

			for _, mq := range m.mqMap {
				mq.Close()
			}

			for _, quit := range m.quits {
				<-quit
			}

			logger.I("ProxyManager", "pchan close")
			m.pchan.Close()
			logger.I("ProxyManager", "all closed")
			return
		}
	}
}
