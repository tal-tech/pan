package store

import (
	"errors"
	"fmt"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"
)

type Storer interface {
	Pop() ([]byte, error)
	Push([]byte) error
	GetWriterOffset() int
	GetReaderOffset() int
	GetStorerType() string
}

var stype = confutil.GetConfStringMap("Pan")["stype"]

type InitStorer func() (Storer, error)

var (
	initStorerMap = make(map[string]InitStorer, 0)
)

func AddStorer(stype string, init InitStorer) {
	initStorerMap[stype] = init
}

func GetStorer() (Storer, error) {
	fmt.Printf("---------store type is %v--------\n", stype)
	if len(initStorerMap) == 0 {
		logger.F("GetStorer", " stype is empty ")
	}
	init := initStorerMap[stype]
	if init != nil {
		storer, err := init()
		if err != nil {
			return nil, err
		}
		return storer, nil
	}

	return nil, errors.New("unsupport store type")
}
