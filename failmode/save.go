/**
 *Created by He Haitao at 2020/7/1 3:23 下午
 */
package failmode

import (
	"context"

	"pan/meta"

	logger "github.com/tal-tech/loggerX"
	redisdao "github.com/tal-tech/xredis"
)

func init() {
	AddFailMode(meta.SAVE, InitSave)
}

func InitSave() (FailMode, error) {
	s := new(Save)
	return s, nil
}

type Save struct {
}

func (s *Save) Do(fallback chan<- []byte, message []byte, data []byte, keyParams []interface{}) {
	fallbackRedis := redisdao.NewSimpleXesRedis(context.Background(), "fallbackRedis")
	_, err := fallbackRedis.LPush("fallback_list_%v_%v", keyParams, []interface{}{string(data)})
	if err != nil {
		logger.E("FailMode Save Do", " error:%v ,message:%v", err, data)
	}
}
