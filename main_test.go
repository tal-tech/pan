package main

import (
	"testing"
)

func TestServer(t *testing.T) {
	/*flagutil.SetConfig("conf/conf.hudong.dev.ini")
	confutil.SetConfPathPrefix(os.Getenv("GOPATH") + "/src/kafkaproxy")
	logger.InitLogger(os.Getenv("GOPATH") + "/src/kafkaproxy/conf/log.xml")
	confutil.InitConfig()
	os.Remove(confutil.GetConf("KafkaProxy", "path"))
	go dealSignal()
	go exec()
	upath := confutil.GetConf("KafkaProxy", "unixSocket")
	for {
		if _, err := os.Stat(upath); os.IsNotExist(err) {
			continue
		}
		break
	}
	var (
		conn net.Conn
		err  error
	)
	now := time.Now().Unix()
	for {
		t.Log("connecting")
		//conn, err := net.DialTimeout("tcp", "127.0.0.1:"+confutil.GetConf("KafkaProxy", "tcpPort"), time.Millisecond*2000)
		conn, err = net.DialTimeout("unix", upath, time.Microsecond*500)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			if time.Now().Unix()-now > 60 {
				t.Fatalf("TestServer dialed too long time,err:%v", err)
			}
			continue
		}
		break
	}
	t.Log("TestServer dial success")
	t.Log("sending")
	s := socket.GetSocket(conn)
	defer func() {
		s.Reset(nil)
		s.Close()
		conn.Close()
	}()
	message := socket.GetMessage()
	defer socket.PutMessage(message)
	message.Reset()
	message.SetBody([]byte("xes_gotest 123456789 xesv5 11 22 33 44 55 66 77"))
	err = s.WriteMessage(message)
	if err != nil {
		t.Fatalf("TestServer write msg failed,err:%v", err)
	}
	message.Reset(socket.WithBody(readerPool.Get().(*[]byte)))
	t.Log("reading")
	err = s.ReadMessage(message)
	if err != nil {
		t.Fatalf("TestServer read msg failed,err:%v", err)
	}
	body, _ := message.MarshalBody()
	back := string(body)
	t.Logf("back:%v", back)
	bb := message.Body().(*[]byte)
	*bb = (*bb)[:0]
	readerPool.Put(bb)
	if !strings.Contains(back, "OK") {
		t.Fatalf("TestServer invalid send back:%s", back)
	}

	pid := os.Getpid()
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("TestServer failed,err:%v", err)
	}
	t.Log("close")
	process.Signal(syscall.SIGTERM)

	time.Sleep(1 * time.Second)*/
}
