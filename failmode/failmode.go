/**
 *Created by He Haitao at 2020/7/1 12:45 下午
 */
package failmode

import (
	"errors"
	"fmt"

	logger "github.com/tal-tech/loggerX"
)

type InitFailMode func() (FailMode, error)

var (
	initFailModeMap = make(map[string]InitFailMode, 0)
)

func AddFailMode(failMode string, init InitFailMode) {
	initFailModeMap[failMode] = init
}

type FailMode interface {
	Do(chan<- []byte, []byte, []byte, []interface{})
}

func GetFailMode(mqType string, failMode string) (FailMode, error) {
	fmt.Printf("-------mqtype is %v ---------fail mode is %v--------\n", mqType, failMode)
	if len(initFailModeMap) == 0 {
		logger.F("GetFailMode", " fail mode is empty ")
	}
	init := initFailModeMap[failMode]
	if init != nil {
		f, err := init()
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	return nil, errors.New("unsupport fail mode")
}
