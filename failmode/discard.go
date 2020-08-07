/**
 *Created by He Haitao at 2020/7/1 3:23 下午
 */
package failmode

import "pan/meta"

func init() {
	AddFailMode(meta.DISCARD, InitDiscard)
}

func InitDiscard() (FailMode, error) {
	d := new(Discard)
	return d, nil
}

type Discard struct {
}

func (d *Discard) Do(fallback chan<- []byte, message []byte, data []byte, keyParams []interface{}) {
}
