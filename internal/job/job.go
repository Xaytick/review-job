package job

import "github.com/google/wire"

var ProviderSet = wire.NewSet(NewJobWorker, NewKafkaReader, NewESClient)