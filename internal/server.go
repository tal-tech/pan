package internal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"pan/meta"

	"git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/addrutil"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"

	"github.com/facebookgo/grace/gracenet"
	"github.com/henrylee2cn/teleport/socket"
	"github.com/spf13/cast"
)

type Server struct {
	mqs                 []string     //已注册mq类型
	tcp                 net.Listener //tcp监听对象
	unix                net.Listener //unixSocket监听对象
	path                string       //socket文件地址
	port                int64
	Input               chan<- []byte   //下游pchan中的input管道
	locker              *sync.RWMutex   //锁
	counter             int64           //成功发送消息计数
	failcnt             int64           //失败消息计数
	validTopic          map[string]bool //kafka rocketmq nsa 白名单
	validExchange       map[string]bool //rabbitmq 白名单
	validKafkaEnable    bool            //kafka白名单是否有效
	validRabbitmqEnable bool
	validRocketmqEnable bool
	validNSQEnable      bool
	exit                chan struct{} //退出信号
	grayMode            bool
}

func NewServer(port, path string, mqs []string, input chan<- []byte) *Server {
	server := new(Server)
	server.path = path
	server.port = cast.ToInt64(port)
	server.mqs = mqs
	server.Input = input
	server.exit = make(chan struct{}, 1)
	if !server.checkTcpPort() && !server.checkUnixSocket() {
		logger.F("ServerInit INIT_ERROR", " TCP OR UNIX_SOCKET SHOULD NOT BE INVALID AT THE SAME TIME! ")
	}
	server.grayMode = cast.ToBool(confutil.GetConfDefault("Pan", "grayMode", "false")) && server.isGrayAddr()
	if server.grayMode {
		fmt.Printf("----------Gray Mode Open-----------\n")
	}
	var err error

	nt := new(gracenet.Net)
	if server.checkTcpPort() {
		server.tcp, err = nt.Listen("tcp", ":"+port)
		if err != nil {
			logger.F("ServerInit LISTEN_ERROR", err)
		}
		go server.waitTcp()
	}
	server.locker = new(sync.RWMutex)
	server.RefreshWhiteList()
	go server.RefreshWhiteListTimer()

	if server.checkUnixSocket() {
		os.Remove(path)
		server.unix, err = nt.Listen("unix", path)
		if err != nil {
			logger.F("ServerInit LISTEN_ERROR", err)
		}
		err = os.Chmod(path, 0777)
		if err != nil {
			logger.F("ServerInit Chmod socket error", err)
		}
		go server.waitUnix()
	}
	return server
}

func (s *Server) checkUnixSocket() bool {
	if s.path == "" || s.path == "/" {
		return false
	}
	return true
}

func (s *Server) checkTcpPort() bool {
	if s.port <= 0 || s.port > 65535 {
		return false
	}
	return true
}

func (s *Server) isGrayAddr() bool {
	ip, err := addrutil.Extract("")
	if err != nil {
		return false
	}
	grayAddress := confutil.GetConf("Pan", "grayAddrs")
	if grayAddress == "" {
		return false
	}
	panMap := confutil.GetConfArrayMap("Pan")
	grayAddressList := panMap["grayAddrs"]
	if ip != "" && len(grayAddressList) > 0 {
		for _, addr := range grayAddressList {
			if addr == ip {
				return true
			}
		}
	}
	return false
}

func (s *Server) RefreshWhiteListTimer() {
	defer meta.Recovery()
	t := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-t.C:
			succ := atomic.SwapInt64(&s.counter, 0)
			fail := atomic.SwapInt64(&s.failcnt, 0)
			logger.I("Refresh", "MESSAGE_REQ succ:%d,fail:%d", succ, fail)
			s.RefreshWhiteList()
		case <-s.exit:
			t.Stop()
			return
		}
	}
}
func (s *Server) RefreshWhiteList() {
	validTopic := make(map[string]bool, 0)
	validExchange := make(map[string]bool, 0)
	for _, v := range s.mqs {
		switch v {
		case meta.Kafka:
			topics := confutil.GetConfs("KafkaProxy", "valid")
			if len(topics) < 1 {
				s.validKafkaEnable = false
				return
			}
			s.validKafkaEnable = true
			for _, topic := range topics {
				validTopic[meta.Kafka+topic] = true
			}
		case meta.Rabbitmq:
			exchanges := confutil.GetConfs("RabbitmqProxy", "valid")
			if len(exchanges) < 1 {
				s.validRabbitmqEnable = false
				return
			}
			s.validRabbitmqEnable = true
			for _, exchange := range exchanges {
				validExchange[meta.Rabbitmq+exchange] = true
			}
		case meta.Rocketmq:
			topics := confutil.GetConfs("RocketmqProxy", "valid")
			if len(topics) < 1 {
				s.validRocketmqEnable = false
				return
			}
			s.validRocketmqEnable = true
			for _, topic := range topics {
				validTopic[meta.Rocketmq+topic] = true
			}
		case meta.NSQ:
			topics := confutil.GetConfs("NSQProxy", "valid")
			if len(topics) < 1 {
				s.validNSQEnable = false
				return
			}
			s.validNSQEnable = true
			for _, topic := range topics {
				validTopic[meta.NSQ+topic] = true
			}
		}
	}
	s.locker.Lock()
	s.validTopic = validTopic
	s.validExchange = validExchange
	s.locker.Unlock()
}

func (s *Server) Close() {
	if s.checkUnixSocket() {
		s.unix.Close()
	}
	if s.checkTcpPort() {
		s.tcp.Close()
	}
	close(s.exit)
}

func (s *Server) dealMsg(s2 socket.Socket, class string) {
	defer meta.Recovery()
	defer s2.Close()
	remote := ""
	if class == "UNIX" {
		remote = "localhost"
	}
	var err error
	defer func() {
		if err != io.EOF {
			logger.E("DealMsgErr", "Err:%v", err)
		}
	}()
	for {
		msg := "ERROR\n"
		message := socket.GetMessage(socket.WithBody(ReaderPool.Get().(*[]byte)))
		err = s2.ReadMessage(message)
		if err != nil {
			if err != io.EOF {
				logger.E(class+"NETSTOP", "%s=>Stoping:%v\n", class, err)
			}
			body := message.Body().(*[]byte)
			*body = (*body)[:0]
			ReaderPool.Put(body)
			socket.PutMessage(message)
			break
		} else {
			line, em := message.MarshalBody()
			if em != nil {
				logger.E(class+"NETSTOP", "%s=>MarshalBody Stoping:%v\n", class, em)
				message.Reset(socket.WithBody([]byte(msg)))
				s2.WriteMessage(message)
				socket.PutMessage(message)
				break
			}
			items := bytes.SplitN(line, []byte(" "), 3)
			serverType := string(message.Meta().Peek("X-MQTYPE")) //通过读取meta中的信息确定消息的类型
			if serverType == "" {
				serverType = meta.Kafka
			}
			if len(items) < 3 {
				logger.W("INVALID_LINE", "%s=>INVALID_LINE:%s", class, line)
				err = errors.New("INVALID_LINE")
			}
			switch serverType {
			case meta.Kafka:
				err = s.kafkaSplit(serverType, line, class, remote)
				if err == nil {
					msg = "OK\n"
				}
			case meta.Rabbitmq:
				err = s.rabbitmqSplit(serverType, line, class, remote)
				if err == nil {
					msg = "OK\n"
				}
			case meta.Rocketmq:
				err = s.rocketSplit(serverType, line, class, remote)
				if err == nil {
					msg = "OK\n"
				}
			case meta.NSQ:
				err = s.nsqSplit(serverType, line, class, remote)
				if err == nil {
					msg = "OK\n"
				}
			case meta.Delay:
				err = s.delaySplit(serverType, line, class)
				if err == nil {
					msg = "OK\n"
				}
			}

			body := message.Body().(*[]byte)
			*body = (*body)[:0]
			ReaderPool.Put(body)
			message.Reset(socket.WithBody([]byte(msg)))
			s2.WriteMessage(message)
		}
		socket.PutMessage(message)
	}
}

func (s *Server) kafkaSplit(serverType string, line []byte, class, remote string) error {
	var err error
	items := bytes.SplitN(line, []byte(" "), 3)
	if len(items) < 3 {
		logger.W("INVALID_LINE", "%s=>INVALID_LINE:%s", class, line)
		err = errors.New("INVALID_LINE")
	} else {
		topic := items[0]
		s.locker.RLock()
		_, ok := s.validTopic[meta.Kafka+string(topic)]
		s.locker.RUnlock()
		if ok || !s.validKafkaEnable {
			loggerid := class + "[" + string(items[1]) + "@" + remote + "]"
			data := items[2]
			if s.grayMode {
				topic = []byte(string(topic) + "_gray")
			}
			lineBuf := BytePool.Get().([]byte)
			lineBuf = append(lineBuf, []byte(cast.ToString(time.Now().Unix()*10))...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, []byte(serverType)...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, topic...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, []byte(loggerid)...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, data...)
			select {
			case s.Input <- lineBuf:
				atomic.AddInt64(&s.counter, int64(1))
			default:
				atomic.AddInt64(&s.failcnt, int64(1))
			}
			return nil
		} else {
			logger.E("INVALID_TOPIC", "%s=>INVALID_LINE:%s", class, line)
			err = errors.New("INVALID_TOPIC")
		}
	}
	return err
}

func (s *Server) delaySplit(serverType string, line []byte, class string) error {
	var err error
	items := bytes.Split(line, []byte(" "))
	if len(items) < 3 {
		logger.W("INVALID_LINE", "%s=>INVALID_LINE:%s", class, line)
		err = errors.New("INVALID_LINE")
	} else {
		lineBuf := BytePool.Get().([]byte)
		lineBuf = append(lineBuf, []byte(cast.ToString(time.Now().Unix()*10))...)
		lineBuf = append(lineBuf, ' ')
		lineBuf = append(lineBuf, []byte(serverType)...)
		lineBuf = append(lineBuf, ' ')
		lineBuf = append(lineBuf, line...)
		select {
		case s.Input <- lineBuf:
			atomic.AddInt64(&s.counter, int64(1))
		default:
			atomic.AddInt64(&s.failcnt, int64(1))
		}
		return nil
	}
	return err
}

func (s *Server) rabbitmqSplit(serverType string, line []byte, class, remote string) error {
	var err error
	items := bytes.SplitN(line, []byte(" "), 4)
	if len(items) < 4 {
		logger.W("INVALID_LINE", "%s=>INVALID_LINE:%s", class, line)
		err = errors.New("INVALID_LINE")
	} else {
		exchange := string(items[0]) + ":" + string(items[1])
		s.locker.RLock()
		_, ok := s.validExchange[meta.Rabbitmq+exchange]
		s.locker.RUnlock()
		if ok || !s.validRabbitmqEnable {
			lineBuf := BytePool.Get().([]byte)
			lineBuf = append(lineBuf, []byte(cast.ToString(time.Now().Unix()*10))...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, []byte(serverType)...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[0]...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[1]...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[2]...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[3]...)

			select {
			case s.Input <- lineBuf:
				atomic.AddInt64(&s.counter, int64(1))
			default:
				atomic.AddInt64(&s.failcnt, int64(1))
			}
			return nil
		} else {
			logger.E("INVALID_EXCHANGE", "%s=>INVALID_LINE:%s", class, line)
			err = errors.New("INVALID_EXCHANGE")
		}
	}
	return err
}

func (s *Server) rocketSplit(serverType string, line []byte, class, remote string) error {
	var err error
	items := bytes.SplitN(line, []byte(" "), 4)
	if len(items) < 4 {
		logger.W("INVALID_LINE", "%s=>INVALID_LINE:%s", class, line)
		err = errors.New("INVALID_LINE")
	} else {
		topic := items[0]
		s.locker.RLock()
		_, ok := s.validTopic[meta.Rocketmq+string(topic)]
		s.locker.RUnlock()
		if ok || !s.validRocketmqEnable {
			lineBuf := BytePool.Get().([]byte)
			lineBuf = append(lineBuf, []byte(cast.ToString(time.Now().Unix()*10))...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, []byte(serverType)...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, topic...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[1]...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[2]...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[3]...)
			select {
			case s.Input <- lineBuf:
				atomic.AddInt64(&s.counter, int64(1))
			default:
				atomic.AddInt64(&s.failcnt, int64(1))
			}
			return nil
		} else {
			logger.E("INVALID_TOPIC", "%s=>INVALID_LINE:%s", class, line)
			err = errors.New("INVALID_TOPIC")
		}
	}
	return err
}

func (s *Server) nsqSplit(serverType string, line []byte, class, remote string) error {
	var err error
	items := bytes.SplitN(line, []byte(" "), 3)
	if len(items) < 3 {
		logger.W("INVALID_LINE", "%s=>INVALID_LINE:%s", class, line)
		err = errors.New("INVALID_LINE")
	} else {
		topic := items[1]
		s.locker.RLock()
		_, ok := s.validTopic[meta.NSQ+string(topic)]
		s.locker.RUnlock()
		if ok || !s.validNSQEnable {
			lineBuf := BytePool.Get().([]byte)
			lineBuf = append(lineBuf, []byte(cast.ToString(time.Now().Unix()*10))...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, []byte(serverType)...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[0]...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, topic...)
			lineBuf = append(lineBuf, ' ')
			lineBuf = append(lineBuf, items[2]...)
			select {
			case s.Input <- lineBuf:
				atomic.AddInt64(&s.counter, int64(1))
			default:
				atomic.AddInt64(&s.failcnt, int64(1))
			}
			return nil
		} else {
			logger.E("INVALID_TOPIC", "%s=>INVALID_LINE:%s", class, line)
			err = errors.New("INVALID_TOPIC")
		}
	}
	return err
}

func (s *Server) waitTcp() {
	for {
		if c, err := s.tcp.Accept(); err == nil {
			go func() {
				s.dealMsg(socket.GetSocket(c), "TCP")
				c.Close()
			}()
		} else {
			if x, ok := err.(*net.OpError); ok && x.Op == "accept" {
				logger.I("TcpNETSTOP", "Stoping Tcp Accept")
				break
			}

			logger.E("TcpNETFAIL", "Accept failed: %v", err)
			continue
		}
	}
}

func (s *Server) waitUnix() {
	defer meta.Recovery()
	for {
		if c, err := s.unix.Accept(); err == nil {
			go func() {
				s.dealMsg(socket.GetSocket(c), "UNIX")
				c.Close()
			}()
		} else {
			if x, ok := err.(*net.OpError); ok && x.Op == "accept" {
				logger.I("UnixNETSTOP", "Stoping Unix Accept")
				break
			}

			logger.E("UnixNETFail", "Accept failed: %v", err)
			continue
		}
	}
}
