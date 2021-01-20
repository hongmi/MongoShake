package tunnel

import (
	executor "mongoshake/elasticsearch_executor"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"mongoshake/common"
)

type ElasticSearchWriter struct {
	RemoteAddrs   []string
	ReplayerId    uint32 // equal to worker-id
	BatchExecutor *executor.BatchGroupExecutor
}

func (writer *ElasticSearchWriter) Name() string {
	return "direct2es"
}

func (writer *ElasticSearchWriter) Prepare() bool {
	nimo.AssertTrue(len(writer.RemoteAddrs) > 0, "RemoteAddrs must > 0")

	//TODO: fill with username and password
	if _, err := utils.TestElasticSearchConn(writer.RemoteAddrs, "TODO", "TODO"); err != nil {
		LOG.Critical("target elasticsearch server[%s] connect failed: %s", writer.RemoteAddrs, err)
		return false
	}

	//urlChoose := writer.ReplayerId % uint32(len(writer.RemoteAddrs))
	writer.BatchExecutor = &executor.BatchGroupExecutor{
		ReplayerId: 		writer.ReplayerId,
		ElasticSearchUrl: 	writer.RemoteAddrs,
	}
	// writer.batchExecutor.RestAPI()
	writer.BatchExecutor.Start()
	return true
}

func (writer *ElasticSearchWriter) Send(message *WMessage) int64 {
	// won't return when Sync has been finished which is a synchronous operation.
	writer.BatchExecutor.Sync(message.ParsedLogs, nil)
	return 0
}

func (writer *ElasticSearchWriter) AckRequired() bool {
	return false
}

func (writer *ElasticSearchWriter) ParsedLogsRequired() bool {
	return true
}
