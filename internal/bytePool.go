package internal

import (
	"sync"
)

var BytePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4096)
		return buf
	},
}
var ReaderPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}
