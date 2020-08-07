/**
 *Created by He Haitao at 2020/7/1 3:23 下午
 */
package failmode

import (
	"pan/meta"
)

func init() {
	AddFailMode(meta.RETRY, InitRetry)
}

func InitRetry() (FailMode, error) {
	r := new(Retry)
	return r, nil
}

type Retry struct {
}

func (r *Retry) Do(fallback chan<- []byte, message []byte, data []byte, keyParams []interface{}) {
	fallback <- message
}
