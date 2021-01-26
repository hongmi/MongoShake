package elasticsearch_executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch"
	"log"
	"mongoshake/common"
	"mongoshake/oplog"
	"strings"
	"time"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
)

// use general bulk interface such like Insert/Update/Delete to execute command
type BulkWriter struct {
	// mongo connection
	session *elasticsearch.Client
	// init sync finish timestamp
	fullFinishTs int64
}

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


type bulkOperation struct {
	action string
	query bson.M
	doc bson.D
}

func (bw *BulkWriter) doBulk(indexName string, ops *[]bulkOperation) error {
	var (
		numErrors  int
		numIndexed int
		raw map[string]interface{}
		blk *bulkResponse
		buf bytes.Buffer
	)

	LOG.Info("[doBulk] begin")
	start := time.Now().UTC()

	for _, op := range *ops {
		//LOG.Info("%v", element)

		if op.action == "delete" {
			var docId string
			docId = op.query["_id"].(bson.ObjectId).Hex()
			meta := []byte(fmt.Sprintf(`{ "%s" : { "_id" : "%s" } }%s`, op.action, docId, "\n"))
			fmt.Printf("[doBulk][meta]%s", meta) // <-- Uncomment to see the payload

			buf.Grow(len(meta))
			buf.Write(meta)
			continue
		}

		//below:
		//  action for "index" and "update"

		m := utils.BsonToMap(&op.doc)
		m["id"] = m["_id"]
		delete(m, "_id")

		var docId string
		if op.action == "index" {
			docId = m["id"].(bson.ObjectId).Hex()
		} else {
			docId = op.query["_id"].(bson.ObjectId).Hex()
		}

		// Prepare the metadata payload
		meta := []byte(fmt.Sprintf(`{ "%s" : { "_id" : "%s" } }%s`, op.action, docId, "\n"))
		fmt.Printf("[doBulk][meta]%s", meta) // <-- Uncomment to see the payload

		data, err := utils.MarshalExtJSONWithRegistry(m)
		//data, err := bson.MarshalJSON(m)
		if err != nil {
			LOG.Error("fail to EMarshalJSON: %v, failed[%v]", m, err)
			return err
		}

		if op.action == "update" {
			dataStr := strings.Trim(string(data),"\n")
			data = []byte(fmt.Sprintf(`{"doc" : %s}`, dataStr))
		}

		// Append newline to the data payload
		data = append(data, "\n"...) // <-- Comment out to trigger failure for batch
		fmt.Printf("[doBulk][data]%s", data) // <-- Uncomment to see the payload

		// Append payloads to the buffer (ignoring write errors)
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}


	res, err := bw.session.Bulk(strings.NewReader(buf.String()), bw.session.Bulk.WithIndex(indexName))
	if err != nil {
		log.Fatalf("Failure indexing %s", err)
		return err
	}
	// If the whole request failed, print error and mark all documents as failed
	if res.IsError() {
		numErrors += len(*ops)
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
			return err
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
			return err
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

	LOG.Info("[doBulk] end")
	return err
}

func (bw *BulkWriter) doInsert(database, collection string, metadata bson.M, oplogs []*OplogRecord,
	dupUpdate bool) error {
	var ops []bulkOperation
	for _, log := range oplogs {
		// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
		newObject := log.original.partialLog.Object
		//inserts = append(inserts, newObject)
		ops = append(ops, bulkOperation{
			action: "index",
			query: nil,
			doc:    newObject,
		})
		LOG.Debug("writer: insert %v", log.original.partialLog)
	}

	indexName := fmt.Sprintf("%s.%s", database, collection)
	return bw.doBulk(indexName, &ops)
}

func (bw *BulkWriter) doUpdateOnInsert(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	var ops []bulkOperation

	for _, log := range oplogs {
		// insert must have _id
		if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
			newObject := log.original.partialLog.Object
			ops = append(ops, bulkOperation{
				action: "index",
				query: bson.M{"_id": id},
				doc:    newObject,
			})
		} else {
			LOG.Warn("Insert on duplicated update _id look up failed. %v", log)
			return fmt.Errorf("Insert on duplicated update _id look up failed. %v", log)
		}

		LOG.Debug("writer: updateOnInsert %v", log.original.partialLog)
	}

	indexName := fmt.Sprintf("%s.%s", database, collection)

	return bw.doBulk(indexName, &ops)
}

func (bw *BulkWriter) doUpdate(database, collection string, metadata bson.M,
	oplogs []*OplogRecord, upsert bool) error {
	var ops []bulkOperation

	for _, log := range oplogs {
		log.original.partialLog.Object = oplog.RemoveFiled(log.original.partialLog.Object, versionMark)
		ops = append(ops, bulkOperation{
			action: "update",
			query:  log.original.partialLog.Query,
			doc:    log.original.partialLog.Object,
		})
		LOG.Debug("writer: update %v, %v", log.original.partialLog.Query, log.original.partialLog.Object)
	}

	indexName := fmt.Sprintf("%s.%s", database, collection)

	return bw.doBulk(indexName, &ops)
}

func (bw *BulkWriter) doDelete(database, collection string, metadata bson.M,
	oplogs []*OplogRecord) error {
	var ops []bulkOperation

	for _, log := range oplogs {
		if id := oplog.GetKey(log.original.partialLog.Object, ""); id != nil {
			ops = append(ops, bulkOperation{
				action: "delete",
				query: bson.M{"_id": id},
				doc:    nil,
			})
		}
		LOG.Debug("writer: delete %v", log.original.partialLog)
	}

	indexName := fmt.Sprintf("%s.%s", database, collection)

	return bw.doBulk(indexName, &ops)
}

func (bw *BulkWriter) doCommand(database string, metadata bson.M, oplogs []*OplogRecord) error {
	//var err error
	//for _, log := range oplogs {
	//	// newObject := utils.AdjustDBRef(log.original.partialLog.Object, conf.Options.DBRef)
	//	newObject := log.original.partialLog.Object
	//	operation, found := oplog.ExtraCommandName(newObject)
	//	if conf.Options.FilterDDLEnable || (found && oplog.IsSyncDataCommand(operation)) {
	//		// execute one by one with sequence order
	//		if err = RunCommand(database, operation, log.original.partialLog, bw.session); err == nil {
	//			LOG.Info("Execute command (op==c) oplog, operation [%s]", operation)
	//		} else if err.Error() == "ns not found" {
	//			LOG.Info("Execute command (op==c) oplog, operation [%s], ignore error[ns not found]", operation)
	//		} else if IgnoreError(err, "c", parseLastTimestamp(oplogs) <= bw.fullFinishTs) {
	//			LOG.Warn("ignore error[%v] when run operation[%v], initialSync[%v]", err, "c", parseLastTimestamp(oplogs) <= bw.fullFinishTs)
	//			return nil
	//		} else {
	//			return err
	//		}
	//	} else {
	//		// exec.batchExecutor.ReplMetric.AddFilter(1)
	//	}
	//
	//	LOG.Debug("writer: command %v", log.original.partialLog)
	//}
	return nil
}