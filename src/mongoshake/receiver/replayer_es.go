package replayer

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/common"
	"mongoshake/modules"
	"mongoshake/oplog"
	"mongoshake/tunnel"
	"strings"
)

const (
	ElasticSearchPendingQueueCapacity = 256
)


type ElasticSearchReplayer struct {
	Retransmit bool  // need re-transmit
	Ack        int64 // ack number

	// current compressor construct by TMessage
	// Compress field specific
	compressor module.Compress

	// pending queue, use to pass message
	pendingQueue chan *ElasticSearchMessageWithCallback

	es *elasticsearch.Client

	id int // current replayer id
}

type ElasticSearchMessageWithCallback struct {
	message    *tunnel.TMessage
	completion func()
}

func getElasticSearchClient() (*elasticsearch.Client) {
	es, _ := elasticsearch.NewDefaultClient()
	return es
}

func NewElasticSearchReplayer(id int) *ElasticSearchReplayer {

	LOG.Info("ElasticSearchReplayer start. pending queue capacity %d", ElasticSearchPendingQueueCapacity)
	er := &ElasticSearchReplayer{
		pendingQueue: make(chan *ElasticSearchMessageWithCallback, ElasticSearchPendingQueueCapacity),
		id:           id,
		es: 		  getElasticSearchClient(),
	}

	go er.handler()
	return er
}

/*
 * Receiver message and do the following steps:
 * 1. if we need re-transmit, this log will be discard
 * 2. validate the checksum
 * 3. decompress
 * 4. put message into channel
 * Generally speaking, do not modify this function.
 */
func (er *ElasticSearchReplayer) Sync(message *tunnel.TMessage, completion func()) int64 {
	// tell collector we need re-trans all unacked oplogs first
	// this always happen on receiver restart !
	if er.Retransmit {
		// reject normal oplogs request
		if message.Tag&tunnel.MsgRetransmission == 0 {
			return tunnel.ReplyRetransmission
		}
		er.Retransmit = false
	}

	// validate the checksum value
	if message.Checksum != 0 {
		recalculated := message.Crc32()
		if recalculated != message.Checksum {
			// we need the peer to retransmission the current message
			er.Retransmit = true
			LOG.Critical("Tunnel message checksum bad. recalculated is 0x%x. origin is 0x%x", recalculated, message.Checksum)
			return tunnel.ReplyChecksumInvalid
		}
	}

	// decompress
	if message.Compress != module.NoCompress {
		// reuse current compressor handle
		var err error
		if er.compressor, err = module.GetCompressorById(message.Compress); err != nil {
			er.Retransmit = true
			LOG.Critical("Tunnel message compressor not support. is %d", message.Compress)
			return tunnel.ReplyCompressorNotSupported
		}
		var decompress [][]byte
		for _, toDecompress := range message.RawLogs {
			bits, err := er.compressor.Decompress(toDecompress)
			if err == nil {
				decompress = append(decompress, bits)
			}
		}
		if len(decompress) != len(message.RawLogs) {
			er.Retransmit = true
			LOG.Critical("Decompress result isn't equivalent. len(decompress) %d, len(Logs) %d", len(decompress), len(message.RawLogs))
			return tunnel.ReplyDecompressInvalid
		}

		message.RawLogs = decompress
	}

	er.pendingQueue <- &ElasticSearchMessageWithCallback{message: message, completion: completion}
	return er.GetAcked()
}

func (er *ElasticSearchReplayer) GetAcked() int64 {
	return er.Ack
}

/*
 * Users should modify this function according to different demands.
 */
func (er *ElasticSearchReplayer) handler() {
	for msg := range er.pendingQueue {
		count := uint64(len(msg.message.RawLogs))
		if count == 0 {
			// probe request
			continue
		}

		// parse batched message
		oplogs := make([]oplog.ParsedLog, len(msg.message.RawLogs))
		for i, raw := range msg.message.RawLogs {
			oplogs[i] = oplog.ParsedLog{}
			if err := bson.Unmarshal(raw, &oplogs[i]); err != nil {
				// impossible switch, need panic and exit
				LOG.Crashf("unmarshal oplog[%v] failed[%v]", raw, err)
				return
			}
			//LOG.Info(oplogs[i]) // just print for test, users can modify to fulfill different needs
			// fmt.Println(oplogs[i])

			er.writeToElasticSearch(oplogs[i])

		}

		if callback := msg.completion; callback != nil {
			callback() // exec callback
		}

		// get the newest timestamp
		n := len(oplogs)
		lastTs := utils.TimestampToInt64(oplogs[n-1].Timestamp)
		er.Ack = lastTs

		LOG.Debug("handle ack[%v]", er.Ack)

		// add logical code below
	}
}

// 假设update操作携带的是full document，要求数据源mongodb版本大于4.0，并且采用changed_stream日志模式
// notice: elasticsearch do not support delete a field,
// so update operation will update specified field only, leave other fields unchanged.
func (er *ElasticSearchReplayer) writeToElasticSearch(olog oplog.ParsedLog) {
	LOG.Info("to sync oplog to es: %v", olog)

	//pasre oplog
	if olog.Operation == "i" {
		er.doInsertToElasticSearch(olog)
	} else if olog.Operation == "d" {
		er.doDeleteToElasticSearch(olog)
	} else if olog.Operation == "u" {
		er.doInsertToElasticSearch(olog)
	} else {
		LOG.Info("ignore a oplog: %+v", olog)
	}
}

func (er *ElasticSearchReplayer) doInsertToElasticSearch(olog oplog.ParsedLog) {
	m := utils.BsonToMap(&olog.Object)
	m["id"] = m["_id"]
	delete(m, "_id")

	buf, err := utils.MarshalExtJSONWithRegistry(m)
	if err != nil {
		LOG.Error("fail to MarshalExtJSONWithRegistry: %v, failed[%v]", m, err)
		return
	}

	indexName := olog.Namespace
	docId := m["id"].(bson.ObjectId).Hex()
	body := strings.NewReader(string(buf))

	req := esapi.IndexRequest{
		Index:               indexName,
		DocumentType:        "",
		DocumentID:          docId,
		Body:                body,
		IfPrimaryTerm:       nil,
		IfSeqNo:             nil,
		OpType:              "",
		Pipeline:            "",
		Refresh:             "",
		RequireAlias:        nil,
		Routing:             "",
		Timeout:             0,
		Version:             nil,
		VersionType:         "",
		WaitForActiveShards: "",
		Pretty:              false,
		Human:               false,
		ErrorTrace:          false,
		FilterPath:          nil,
		Header:              nil,
	}
	res, err := req.Do(context.Background(), er.es)
	if err != nil {
		LOG.Error("fail to insert a document to elasticsearch: %v, failed[%v]", m, err)
		return
	}
	LOG.Info("%v", res)
}

func (er *ElasticSearchReplayer) doDeleteToElasticSearch(olog oplog.ParsedLog) {
	m := olog.Object.Map()

	indexName := olog.Namespace
	docId := m["_id"].(bson.ObjectId).Hex()

	req := esapi.DeleteRequest{
		Index:               indexName,
		DocumentType:        "",
		DocumentID:          docId,
		IfPrimaryTerm:       nil,
		IfSeqNo:             nil,
		Refresh:             "",
		Routing:             "",
		Timeout:             0,
		Version:             nil,
		VersionType:         "",
		WaitForActiveShards: "",
		Pretty:              false,
		Human:               false,
		ErrorTrace:          false,
		FilterPath:          nil,
		Header:              nil,
	}
	res, err := req.Do(context.Background(), er.es)
	if err != nil {
		LOG.Error("fail to delete a document to elasticsearch: %v, failed[%v]", m, err)
		return
	}
	LOG.Info("%v", res)
}

