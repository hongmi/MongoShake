package elasticsearch_executor

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"testing"

	"mongoshake/common"
	"mongoshake/oplog"
	"mongoshake/unit_test_common"

	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
)

const (
	testMongoAddress = unit_test_common.TestUrl
	testDb           = "writer_test"
	testCollection   = "a"
)

func mockOplogRecord(oId, oX int, o2Id int) *OplogRecord {
	or := &OplogRecord{
		original: &PartialLogWithCallbak {
			partialLog: &oplog.PartialLog {
				ParsedLog: oplog.ParsedLog {
					Object: bson.D{
						bson.DocElem{
							Name: "_id",
							Value: oId,
						},
						bson.DocElem{
							Name: "x",
							Value: oX,
						},
					},
				},
			},
		},
	}

	if o2Id != -1 {
		or.original.partialLog.ParsedLog.Query = bson.M {
			"_id": o2Id,
		}
	}

	return or
}

func TestBulkWriter(t *testing.T) {
	// test bulk writer
	conn, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:             []string{testMongoAddress},
		Username:              "",
		Password:              "",
		CloudID:               "",
		APIKey:                "",
		Header:                nil,
		CACert:                nil,
		RetryOnStatus:         nil,
		DisableRetry:          false,
		EnableRetryOnTimeout:  false,
		MaxRetries:            0,
		DiscoverNodesOnStart:  false,
		DiscoverNodesInterval: 0,
		EnableMetrics:         false,
		EnableDebugLogger:     false,
		RetryBackoff:          nil,
		Transport:             nil,
		Logger:                nil,
		Selector:              nil,
		ConnectionPoolFunc:    nil,
	})
	assert.Equal(t, nil, err, "should be equal")

	var nr int

	// basic test
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		// drop database
		req := esapi.IndicesDeleteRequest{
			Index:             []string{fmt.Sprintf("%s.%s", testDb, testCollection)},
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
		_, err = req.Do(context.Background(), conn)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// 4-8
		inserts = []*OplogRecord{
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
			mockOplogRecord(6, 6, -1),
			mockOplogRecord(7, 7, -1),
			mockOplogRecord(8, 8, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// query
		result := make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)

		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 8, len(result), "should be equal")
		assert.Equal(t, 1, result[0].(bson.M)["x"], "should be equal")
		assert.Equal(t, 2, result[1].(bson.M)["x"], "should be equal")
		assert.Equal(t, 3, result[2].(bson.M)["x"], "should be equal")
		assert.Equal(t, 4, result[3].(bson.M)["x"], "should be equal")
		assert.Equal(t, 5, result[4].(bson.M)["x"], "should be equal")
		assert.Equal(t, 6, result[5].(bson.M)["x"], "should be equal")
		assert.Equal(t, 7, result[6].(bson.M)["x"], "should be equal")
		assert.Equal(t, 8, result[7].(bson.M)["x"], "should be equal")

		// 8-10
		inserts = []*OplogRecord{
			mockOplogRecord(8, 80, -1),
			mockOplogRecord(9, 90, -1),
			mockOplogRecord(10, 100, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, true)
		assert.Equal(t, nil, err, "should be equal")

		// query
		result = make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)

		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 10, len(result), "should be equal")
		assert.Equal(t, 80, result[7].(bson.M)["x"], "should be equal")
		assert.Equal(t, 90, result[8].(bson.M)["x"], "should be equal")
		assert.Equal(t, 100, result[9].(bson.M)["x"], "should be equal")

		// delete 8-11
		deletes := []*OplogRecord{
			mockOplogRecord(8, 80, -1),
			mockOplogRecord(9, 90, -1),
			mockOplogRecord(10, 100, -1),
			mockOplogRecord(11, 110, -1), // not found
		}
		err = writer.doDelete(testDb, testCollection, bson.M{}, deletes)
		assert.Equal(t, nil, err, "should be equal") // won't throw error if not found

		result = make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")

	}

	// bulk update, delete
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		// drop database
		req := esapi.IndicesDeleteRequest{
			Index:             []string{fmt.Sprintf("%s.%s", testDb, testCollection)},
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
		_, err = req.Do(context.Background(), conn)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// update not exist
		updates := []*OplogRecord{
			mockOplogRecord(5, 50, 5),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// not work
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, false)
		assert.Equal(t, nil, err, "should be equal")

		result := make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, 50, result[4].(bson.M)["x"], "should be equal")

		// updates
		updates = []*OplogRecord{
			mockOplogRecord(4, 40, 4),
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		result = make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, 40, result[3].(bson.M)["x"], "should be equal")
		assert.Equal(t, 50, result[4].(bson.M)["x"], "should be equal")
		assert.Equal(t, 100, result[5].(bson.M)["x"], "should be equal")
		assert.Equal(t, 110, result[6].(bson.M)["x"], "should be equal")

		// deletes
		deletes := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(999, 999, -1), // not exist
		}

		err = writer.doDelete(testDb, testCollection, bson.M{}, deletes)
		result = make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
	}

	// bulk update, delete
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		// drop database
		req := esapi.IndicesDeleteRequest{
			Index:             []string{fmt.Sprintf("%s.%s", testDb, testCollection)},
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
		_, err = req.Do(context.Background(), conn)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, -1)

		// 1-5
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			mockOplogRecord(4, 4, -1),
			mockOplogRecord(5, 5, -1),
		}

		// write 1
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		// build index
		//err = conn.Session.DB(testDb).C(testCollection).EnsureIndex(mgo.Index{
		//	Key: []string{"x"},
		//	Unique: true,
		//})
		//assert.Equal(t, nil, err, "should be equal")

		// updates
		updates := []*OplogRecord{
			mockOplogRecord(3, 5, 3), // dup
			mockOplogRecord(10, 100, 10),
			mockOplogRecord(11, 110, 11),
		}

		// upsert = false
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, false)
		assert.NotEqual(t, nil, err, "should be equal")
		fmt.Println(err)

		result := make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 5, len(result), "should be equal")
		assert.Equal(t, 3, result[2].(bson.M)["x"], "should be equal")

		// upsert
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		result = make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 7, len(result), "should be equal")
		assert.Equal(t, 3, result[2].(bson.M)["x"], "should be equal")
		assert.Equal(t, 100, result[5].(bson.M)["x"], "should be equal")
		assert.Equal(t, 110, result[6].(bson.M)["x"], "should be equal")
	}

	// test ignore error
	{
		fmt.Printf("TestBulkWriter case %d.\n", nr)
		nr++

		utils.InitialLogger("", "", "info", true, true)

		// drop database
		req := esapi.IndicesDeleteRequest{
			Index:             []string{fmt.Sprintf("%s.%s", testDb, testCollection)},
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
		_, err = req.Do(context.Background(), conn)
		assert.Equal(t, nil, err, "should be equal")

		writer := NewDbWriter(conn, bson.M{}, true, 100)
		inserts := []*OplogRecord{
			mockOplogRecord(1, 1, -1),
			mockOplogRecord(2, 2, -1),
			mockOplogRecord(3, 3, -1),
			{
				original: &PartialLogWithCallbak {
					partialLog: &oplog.PartialLog {
						ParsedLog: oplog.ParsedLog {
							Object: bson.D{
								bson.DocElem{
									Name: "_id",
									Value: 110011,
								},
								bson.DocElem{
									Name: "x",
									Value: nil,
								},
							},
						},
					},
				},
			},
		}
		err = writer.doInsert(testDb, testCollection, bson.M{}, inserts, false)
		assert.Equal(t, nil, err, "should be equal")

		result := make([]interface{}, 0)
		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3].(bson.M)["x"], "should be equal")

		updates := []*OplogRecord{
			mockOplogRecord(1, 10, 1),
			{
				original: &PartialLogWithCallbak {
					partialLog: &oplog.PartialLog {
						ParsedLog: oplog.ParsedLog {
							Timestamp: 1,
							Object: bson.D{
								bson.DocElem{
									Name: "$set",
									Value: bson.M{
										"x.0.y": 123,
									},
								},
							},
							Query: bson.M{
								"_id": 110011,
							},
						},
					},
				},
			},
			mockOplogRecord(2, 20, 2),
			mockOplogRecord(3, 30, 3),
		}
		err = writer.doUpdate(testDb, testCollection, bson.M{}, updates, true)
		assert.Equal(t, nil, err, "should be equal")

		//TODO: retrieve from elasticsearch
		//err = conn.Session.DB(testDb).C(testCollection).Find(bson.M{}).Sort("_id").All(&result)
		fmt.Println(result)
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 4, len(result), "should be equal")
		assert.Equal(t, nil, result[3].(bson.M)["x"], "should be equal")
		assert.Equal(t, 10, result[0].(bson.M)["x"], "should be equal")
		assert.Equal(t, 20, result[1].(bson.M)["x"], "should be equal")
		assert.Equal(t, 30, result[2].(bson.M)["x"], "should be equal")
	}
}

func TestIgnoreError(t *testing.T) {
	// test IgnoreError

	var nr int

	// applyOps
	{
		fmt.Printf("TestIgnoreError case %d.\n", nr)
		nr++

		var err error = &mgo.LastError{Code: 26}
		ignore := IgnoreError(err, "d", false)
		assert.Equal(t, true, ignore, "should be equal")

		err = &mgo.QueryError{Code: 280}
		ignore = IgnoreError(err, "d", false)
		assert.Equal(t, false, ignore, "should be equal")
	}
}

