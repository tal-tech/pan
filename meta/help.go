/**
 *Created by He Haitao at 2019/11/27 10:36 上午
 */
package meta

import (
	"fmt"
	"runtime/debug"

	"github.com/spf13/cast"
)

func Recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			fmt.Printf("PanicRecover Unhandled error: %v stack:%v\n", err.Error(), cast.ToString(debug.Stack()))
		} else {
			fmt.Printf("PanicRecover Panic: %v stack:%v", rec, cast.ToString(debug.Stack()))
		}
	}
}
