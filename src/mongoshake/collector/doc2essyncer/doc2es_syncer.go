package doc2essyncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mongoshake/collector/ckpt"
	"mongoshake/collector/configure"
	"mongoshake/collector/transform"
	"mongoshake/common"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

const (
	MAX_BUFFER_BYTE_SIZE = 16 * 1024 * 1024
	SpliterReader        = 4
)

func StartDropDestIndex(nsSet map[utils.NS]struct{}, toConn *elasticsearch.Client,
	nsTrans *transform.NamespaceTransform) error {
	for ns := range nsSet {
		toNS := utils.NewNS(nsTrans.Transform(ns.Str()))
		if !conf.Options.FullSyncCollectionDrop {
			// do not drop
			req := esapi.CatIndicesRequest{}
			res, err := req.Do(context.Background(), toConn)
			log.Println(res)
			if err != nil {
				LOG.Critical("Get indices names of dest elasticsearch failed. %v", err)
				return err
			}

			var dat []map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&dat); err != nil {
				log.Fatalf("Failure to to parse response body: %s", err)
			} else {
				for _, d := range dat {
					log.Println(d["index"])
					// judge whether toNs exists
					if d["index"] == toNS.Str() {
						LOG.Warn("ns %v to be synced already exists in dest elasticsearch", toNS)
						break
					}
				}
			}
			res.Body.Close()

		} else {
			// need drop
			req := esapi.IndicesDeleteRequest{
				Index:             []string{toNS.Str()},
				AllowNoIndices:    nil,
				ExpandWildcards:   "",
				IgnoreUnavailable: nil,
				MasterTimeout:     0,
				Timeout:           0,
				Pretty:            false,
				Human:             false,
				ErrorTrace:        false,
				FilterPath:        nil,
				Header:            nil,
			}
			res, err := req.Do(context.Background(), toConn)
			if err != nil && res.StatusCode == http.StatusNotFound{
				LOG.Critical("Drop index ns %v of dest elasticsearch failed. %v", toNS, err)
				return errors.New(fmt.Sprintf("Drop index ns %v of dest elasticsearch failed. %v", toNS, err))
			}
		}
	}
	return nil
}

func Checkpoint(ckptMap map[string]utils.TimestampNode) error {
	for name, ts := range ckptMap {
		ckptManager := ckpt.NewCheckpointManager(name, 0)
		ckptManager.Get() // load checkpoint in ckptManager
		if err := ckptManager.Update(ts.Newest); err != nil {
			return err
		}
	}
	return nil
}

/************************************************************************/
// 1 shard -> 1 DBSyncer
type DBSyncer struct {
	// syncer id
	id int
	// source mongodb url
	FromMongoUrl string
	fromReplset string
	// destination elasticsearch url
	ToESUrl []string
	// start time of sync
	startTime time.Time

	nsTrans *transform.NamespaceTransform

	mutex sync.Mutex

	qos *utils.Qos // not owned

	replMetric *utils.ReplicationMetric

	// below are metric info
	metricNsMapLock sync.Mutex
	metricNsMap     map[utils.NS]*CollectionMetric // namespace map: db.collection -> collection metric
}

func NewDBSyncer(
	id int,
	fromMongoUrl string,
	fromReplset string,
	toESUrl []string,
	nsTrans *transform.NamespaceTransform,
	qos *utils.Qos) *DBSyncer {

	syncer := &DBSyncer{
		id:             id,
		FromMongoUrl:   fromMongoUrl,
		fromReplset:    fromReplset,
		ToESUrl:     	toESUrl,
		nsTrans:        nsTrans,
		qos:            qos,
		metricNsMap:    make(map[utils.NS]*CollectionMetric),
		replMetric:     utils.NewMetric(fromReplset, utils.TypeFull, utils.METRIC_TPS),
	}

	return syncer
}

func (syncer *DBSyncer) String() string {
	return fmt.Sprintf("DBSyncer id[%v] source[%v] target[%v] startTime[%v]",
		syncer.id, utils.BlockMongoUrlPassword(syncer.FromMongoUrl, "***"),
		strings.Join(syncer.ToESUrl, ";"), syncer.startTime)
}

func (syncer *DBSyncer) Init() {
	syncer.RestAPI()
}

func (syncer *DBSyncer) Close() {
	LOG.Info("syncer[%v] closed", syncer)
	syncer.replMetric.Close()
	//sleep 1s for metric close finnally!
	time.Sleep(1 * time.Second)
}

func (syncer *DBSyncer) Start() (syncError error) {
	syncer.startTime = time.Now()
	var wg sync.WaitGroup

	// get all namespace
	nsList, _, err := GetDbNamespace(syncer.FromMongoUrl)
	if err != nil {
		return err
	}

	if len(nsList) == 0 {
		LOG.Info("%s finish, but no data", syncer)
		return
	}

	// create metric for each collection
	for _, ns := range nsList {
		syncer.metricNsMap[ns] = NewCollectionMetric()
	}

	collExecutorParallel := conf.Options.FullSyncReaderCollectionParallel
	namespaces := make(chan utils.NS, collExecutorParallel)

	wg.Add(len(nsList))

	nimo.GoRoutine(func() {
		for _, ns := range nsList {
			namespaces <- ns
		}
	})

	// run collection sync in parallel
	var nsDoneCount int32 = 0
	for i := 0; i < collExecutorParallel; i++ {
		collExecutorId := GenerateCollExecutorId()
		nimo.GoRoutine(func() {
			for {
				ns, ok := <-namespaces
				if !ok {
					break
				}

				toNS := utils.NewNS(syncer.nsTrans.Transform(ns.Str()))

				LOG.Info("%s collExecutor-%d sync ns %v to %v begin", syncer, collExecutorId, ns, toNS)
				err := syncer.collectionSync(collExecutorId, ns, toNS)
				atomic.AddInt32(&nsDoneCount, 1)

				if err != nil {
					LOG.Critical("%s collExecutor-%d sync ns %v to %v failed. %v",
						syncer, collExecutorId, ns, toNS, err)
					syncError = fmt.Errorf("document syncer sync ns %v to %v failed. %v", ns, toNS, err)
				} else {
					process := int(atomic.LoadInt32(&nsDoneCount)) * 100 / len(nsList)
					LOG.Info("%s collExecutor-%d sync ns %v to %v successful. db syncer-%d progress %v%%",
						syncer, collExecutorId, ns, toNS, syncer.id, process)
				}
				wg.Done()
			}
			LOG.Info("%s collExecutor-%d finish", syncer, collExecutorId)
		})
	}

	wg.Wait()
	close(namespaces)

	return syncError
}

// start sync single collection
func (syncer *DBSyncer) collectionSync(collExecutorId int, ns utils.NS, toNS utils.NS) error {
	// writer
	colExecutor := NewCollectionExecutor(collExecutorId, syncer.ToESUrl, toNS, syncer)
	if err := colExecutor.Start(); err != nil {
		return err
	}

	// splitter reader
	splitter := NewDocumentSplitter(syncer.FromMongoUrl, ns)
	if splitter == nil {
		return fmt.Errorf("create splitter failed")
	}
	defer splitter.Close()

	// metric
	collectionMetric := syncer.metricNsMap[ns]
	collectionMetric.CollectionStatus = StatusProcessing
	collectionMetric.TotalCount = splitter.count

	// run in several pieces
	var wg sync.WaitGroup
	wg.Add(splitter.pieceNumber)
	for i := 0; i < SpliterReader; i++ {
		go func() {
			for {
				reader, ok := <-splitter.readerChan
				if !ok {
					break
				}

				if err := syncer.splitSync(reader, colExecutor, collectionMetric); err != nil {
					LOG.Crashf("%v", err)
				}

				wg.Done()
			}
		}()
	}
	wg.Wait()

	// close writer
	if err := colExecutor.Wait(); err != nil {
		return fmt.Errorf("close writer failed: %v", err)
	}

	/*
	 * in the former version, we fetch indexes after all data finished. However, it'll
	 * have problem if the index is build/delete/update in the full-sync stage, the oplog
	 * will be replayed again, e.g., build index, which must be wrong.
	 */
	// fetch index

	// set collection finish
	collectionMetric.CollectionStatus = StatusFinish

	return nil
}

func (syncer *DBSyncer) splitSync(reader *DocumentReader, colExecutor *CollectionExecutor, collectionMetric *CollectionMetric) error {
	bufferSize := conf.Options.FullSyncReaderDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)
	bufferByteSize := 0

	for {
		doc, err := reader.NextDoc()
		if err != nil {
			return fmt.Errorf("splitter reader[%v] get next document failed: %v", reader, err)
		} else if doc == nil {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			break
		}

		syncer.replMetric.AddGet(1)
		syncer.replMetric.AddSuccess(1) // only used to calculate the tps which is extract from "success"

		if bufferByteSize+len(doc.Data) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		// transform dbref for document
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.IncrSyncDBRef {
			var docData bson.D
			if err := bson.Unmarshal(doc.Data, docData); err != nil {
				LOG.Error("splitter reader[%v] do bson unmarshal %v failed. %v", reader, doc.Data, err)
			} else {
				docData = transform.TransformDBRef(docData, reader.ns.Database, syncer.nsTrans)
				if v, err := bson.Marshal(docData); err != nil {
					LOG.Warn("splitter reader[%v] do bson marshal %v failed. %v", reader, docData, err)
				} else {
					doc.Data = v
				}
			}
		}
		buffer = append(buffer, doc)
		bufferByteSize += len(doc.Data)
	}

	LOG.Info("splitter reader finishes: %v", reader)
	reader.Close()
	return nil
}

/************************************************************************/
// restful api
func (syncer *DBSyncer) RestAPI() {
	// progress api
	type OverviewInfo struct {
		Progress             string            `json:"progress"`                     // synced_collection_number / total_collection_number
		TotalCollection      int               `json:"total_collection_number"`      // total collection
		FinishedCollection   int               `json:"finished_collection_number"`   // finished
		ProcessingCollection int               `json:"processing_collection_number"` // in processing
		WaitCollection       int               `json:"wait_collection_number"`       // wait start
		CollectionMetric     map[string]string `json:"collection_metric"`            // collection_name -> process
	}

	utils.FullSyncHttpApi.RegisterAPI("/progress", nimo.HttpGet, func([]byte) interface{} {
		ret := OverviewInfo{
			CollectionMetric: make(map[string]string),
		}

		syncer.metricNsMapLock.Lock()
		defer syncer.metricNsMapLock.Unlock()

		ret.TotalCollection = len(syncer.metricNsMap)
		for ns, collectionMetric := range syncer.metricNsMap {
			ret.CollectionMetric[ns.Str()] = collectionMetric.String()
			switch collectionMetric.CollectionStatus {
			case StatusWaitStart:
				ret.WaitCollection += 1
			case StatusProcessing:
				ret.ProcessingCollection += 1
			case StatusFinish:
				ret.FinishedCollection += 1
			}
		}

		if ret.TotalCollection == 0 {
			ret.Progress = "100%"
		} else {
			ret.Progress = fmt.Sprintf("%.2f%%", float64(ret.FinishedCollection) / float64(ret.TotalCollection) * 100)
		}

		return ret
	})

	/***************************************************/


}
