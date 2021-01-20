package doc2essyncer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	"mongoshake/collector/configure"
	"mongoshake/common"
	"sync"
)

var (
	GlobalCollExecutorId int32 = -1
	GlobalDocExecutorId int32 = -1
)

type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	id int
	// elasticsearch url
	toEsUrl []string

	ns utils.NS

	wg sync.WaitGroup
	// batchCount int64

	docBatch chan []*bson.Raw

	// not own
	syncer *DBSyncer
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutor(id int, toEsUrl []string, ns utils.NS, syncer *DBSyncer) *CollectionExecutor {
	return &CollectionExecutor{
		id:         id,
		toEsUrl:   toEsUrl,
		ns:         ns,
		syncer:     syncer,
		// batchCount: 0,
	}
}

func (colExecutor *CollectionExecutor) Start() error {
	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		// Use a third-party package for implementing the backoff function
		retryBackoff := backoff.NewExponentialBackOff()
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		// Create the Elasticsearch client
		es, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: conf.Options.TunnelAddress,
			// Retry on 429 TooManyRequests statuses
			RetryOnStatus: []int{502, 503, 504, 429},
			// Configure the backoff function
			RetryBackoff: func(i int) time.Duration {
				if i == 1 {
					retryBackoff.Reset()
				}
				return retryBackoff.NextBackOff()
			},
			// Retry up to 5 attempts
			MaxRetries: 5,
		})
		if err != nil {
			LOG.Error("Error creating the client: %s", err)
		}
		// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, es, colExecutor.syncer)
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

func (colExecutor *CollectionExecutor) Sync(docs []*bson.Raw) {
	count := uint64(len(docs))
	if count == 0 {
		return
	}

	/*
	 * TODO, waitGroup.Add may overflow, so use atomic to replace waitGroup
	 * // colExecutor.wg.Add(1)
	 */
	colExecutor.wg.Add(1)
	// atomic.AddInt64(&colExecutor.batchCount, 1)
	colExecutor.docBatch <- docs
}

func (colExecutor *CollectionExecutor) Wait() error {
	colExecutor.wg.Wait()
	/*for v := atomic.LoadInt64(&colExecutor.batchCount); v != 0; {
		utils.YieldInMs(1000)
		LOG.Info("CollectionExecutor[%v %v] wait batchCount[%v] == 0", colExecutor.ns, colExecutor.id, v)
	}*/

	close(colExecutor.docBatch)
	//colExecutor.conn.Close()

	for _, exec := range colExecutor.executors {
		if exec.error != nil {
			return errors.New(fmt.Sprintf("sync ns %v failed. %v", colExecutor.ns, exec.error))
		}
	}
	return nil
}

type DocExecutor struct {
	// sequence index id in each replayer
	id int
	// colExecutor, not owned
	colExecutor *CollectionExecutor

	es *elasticsearch.Client

	error error

	// not own
	syncer *DBSyncer
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, es *elasticsearch.Client, syncer *DBSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		es:  		 es,
		syncer:      syncer,
	}
}

func (exec *DocExecutor) String() string {
	return fmt.Sprintf("DocExecutor[%v] collectionExecutor[%v]", exec.id, exec.colExecutor.ns)
}

func (exec *DocExecutor) start() {
	//defer exec.es.close()
	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
				// since v2.4.11: panic directly if meets error
				LOG.Crashf("%s sync failed: %v", exec, err)
			}
		}

		exec.colExecutor.wg.Done()
		// atomic.AddInt64(&exec.colExecutor.batchCount, -1)
	}
}

func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var docList []interface{}
	for _, doc := range docs {
		docList = append(docList, doc)
	}

	// qps limit if enable
	if exec.syncer.qos.Limit > 0 {
		exec.syncer.qos.FetchBucket()
	}

	if conf.Options.LogLevel == utils.VarLogLevelDebug {
		var docBeg, docEnd bson.M
		bson.Unmarshal(docs[0].Data, &docBeg)
		bson.Unmarshal(docs[len(docs) - 1].Data, &docEnd)
		LOG.Debug("DBSyncer id[%v] doSync with table[%v] batch _id interval [%v, %v]", exec.syncer.id, ns,
			docBeg["_id"], docEnd["_id"])
	}

	exec.writeToElasticSearch(docs)

	return nil
}

//func convertToJsonObj(bo interface{}) (jo interface{}) {
//	switch t := bo.(type) {
//	case bson.D:
//		return bsonToMap(&t)
//	default:
//		return t
//	}
//}
//
//func bsonToMap(pd *bson.D) (m bson.M) {
//	d := *pd
//	m = make(bson.M, len(d))
//	for _, item := range d {
//		switch t := item.Value.(type) {
//		case []interface{}:
//			var a []interface{}
//			for _, i := range t {
//				a = append(a, convertToJsonObj(i))
//			}
//			m[item.Name] = a
//		case bson.D:
//			m[item.Name] = bsonToMap(&t)
//		default:
//			m[item.Name] = item.Value
//
//		}
//	}
//	return m
//}

//注意：当数据结构里包含数组，而且数组元素的类型不一致的时候，索引失败。不知道mongo-connector能否正常，待研究？
func (exec *DocExecutor) writeToElasticSearch(docs []*bson.Raw) {

	LOG.Info("[writeToElasticSearch] begin")
	var (
		numItems   int
		numErrors  int
		numIndexed int
		raw map[string]interface{}
		blk *bulkResponse
		buf bytes.Buffer
	)

	var docD bson.D

	start := time.Now().UTC()

	for _, element := range docs {
		numItems++

		bson.Unmarshal(element.Data, &docD)
		//LOG.Info("%v", docD)
		m := utils.BsonToMap(&docD)
		m["id"] = m["_id"]
		delete(m, "_id")

		docId := m["id"].(bson.ObjectId).Hex()

		// Prepare the metadata payload
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, docId, "\n"))
		// fmt.Printf("%s", meta) // <-- Uncomment to see the payload

		data, err := utils.MarshalExtJSONWithRegistry(m)
		//data, err := bson.MarshalJSON(m)
		if err != nil {
			LOG.Error("fail to EMarshalJSON: %v, failed[%v]", m, err)
			return
		}

		// Append newline to the data payload
		data = append(data, "\n"...) // <-- Comment out to trigger failure for batch
		// fmt.Printf("%s", data) // <-- Uncomment to see the payload

		// Append payloads to the buffer (ignoring write errors)
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	//LOG.Info(buf.String())

	ns := exec.colExecutor.ns
	indexName := ns.Str()

	res, err := exec.es.Bulk(strings.NewReader(buf.String()), exec.es.Bulk.WithIndex(indexName))
	if err != nil {
		log.Fatalf("Failure indexing %s", err)
	}
	// If the whole request failed, print error and mark all documents as failed
	if res.IsError() {
		numErrors += numItems
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			log.Printf("  Error: [%d] %s: %s",
				res.StatusCode,
				raw["error"].(map[string]interface{})["type"],
				raw["error"].(map[string]interface{})["reason"],
			)
		}
		// A successful response might still contain errors for particular documents...
	} else {
		if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			for _, d := range blk.Items {
				// ... so for any HTTP status above 201 ...
				if d.Index.Status > 201 {
					// ... increment the error counter ...
					numErrors++

					// ... and print the response status and error information ...
					log.Printf("  Error: [%d]: %s: %s: %s: %s",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
				} else {
					// ... otherwise increase the success counter.
					numIndexed++
				}
			}
		}
	}

	// Close the response body, to prevent reaching the limit for goroutines or file handles
	//
	res.Body.Close()

	dur := time.Since(start)

	if numErrors > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	}

	LOG.Info("[writeToElasticSearch] end")
}
