package global

import (
	"sync"

	"github.com/geometry-labs/go-service-template/crud"
)

const Version = "v0.1.0"

type Global struct {
	Blocks *crud.BlockModel
}

var globalInstance *Global
var globalOnce sync.Once

func GetGlobal() *Global {
	globalOnce.Do(func() {
		globalInstance = &Global{
			Blocks: crud.GetBlockModel(),
		}
	})
	return globalInstance
}

var ShutdownChan = make(chan int)
