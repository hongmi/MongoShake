package replayer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/esutil"
	LOG "github.com/vinllen/log4go"
	"log"
	utils "mongoshake/common"
	"net/http"
	"strings"
	"testing"

	"mongoshake/oplog"
	"mongoshake/tunnel"

	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"time"
)



func TestTrial(t *testing.T) {
	var a interface{}
	a = bson.ObjectIdHex("5feaedff8ae1b3bdad89801e")
	switch tp := a.(type) {
	case bson.ObjectId:
		fmt.Println("ssss")
	default:
		fmt.Println(tp)
	}

	e := bson.D{
		bson.DocElem{
			Name:  "_id",
			Value: bson.ObjectIdHex("5feaedff8ae1b3bdad89801e"),
		},
		bson.DocElem{
			Name: "a",
			Value: time.Unix(0,1),
		},
	}

	//d := official_bson.D{{"foo", "bar"}, {"hello", primitive.DateTime(1)}, {"pi", 3.14159}}

	j, err := utils.MarshalExtJSONWithRegistry(utils.BsonToMap(&e))
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	fmt.Println(string(j))

	assert.Equal(t, 1, 1, "should be equal")
}
func TestEsSearch(t *testing.T) {
	es, _ := elasticsearch.NewDefaultClient()
	log.Println(elasticsearch.Version)
	log.Println(es.Info())
	sr := esapi.SearchRequest{
		Index: []string{"double11"},
	}
	res, err := sr.Do(context.Background(), es)
	log.Println(res)
	log.Println(err)



	assert.Equal(t, 1, 1, "should be equal")
}

func TestEsAuth(t *testing.T) {
	es, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:             []string{"http://localhost:9200/"},
		Username:              "elastic",
		Password:              "elastic",
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
	log.Println(elasticsearch.Version)
	log.Println(es.Info())
	sr := esapi.SearchRequest{
		Index: []string{"double11"},
	}
	res, err := sr.Do(context.Background(), es)
	log.Println(res)
	log.Println(err)

	assert.Equal(t, 1, 1, "should be equal")
}


func TestEsDeleteIndex(t *testing.T) {
	es, _ := elasticsearch.NewDefaultClient()

	indexName := "aaa"

	req := esapi.IndicesDeleteRequest{
		Index:             []string{"aaa"},
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
	res, err := req.Do(context.Background(), es)
	log.Println(res)
	if res.StatusCode == http.StatusNotFound {
		LOG.Critical("[index not found]Drop index ns %v failed. %v", indexName, err)
	}
	if err != nil {
		LOG.Critical("Drop index ns %v failed. %v", indexName, err)
	}
}

func TestEsHello(t *testing.T) {
	es, _ := elasticsearch.NewDefaultClient()
	log.Println(elasticsearch.Version)
	log.Println(es.Info())

	req := esapi.CatIndicesRequest{
		Index:                   nil,
		Bytes:                   "",
		ExpandWildcards:         "",
		Format:                  "JSON",
		H:                       nil,
		Health:                  "",
		Help:                    nil,
		IncludeUnloadedSegments: nil,
		Local:                   nil,
		MasterTimeout:           0,
		Pri:                     nil,
		S:                       nil,
		Time:                    "",
		V:                       nil,
		Pretty:                  false,
		Human:                   false,
		ErrorTrace:              false,
		FilterPath:              nil,
		Header:                  nil,
	}
	res, err := req.Do(context.Background(), es)
	log.Println(res)
	log.Println(err)

	var dat []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&dat); err != nil {
		log.Fatalf("Failure to to parse response body: %s", err)
	} else {
		for _, d := range dat {
			log.Println(d["index"])
		}
	}
	res.Body.Close()


	assert.Equal(t, 1, 1, "should be equal")

}

func TestEsBulk(t *testing.T) {
	es, _ := elasticsearch.NewDefaultClient()

	bic := esutil.BulkIndexerConfig{
		NumWorkers:          1,
		FlushBytes:          10000000,
		FlushInterval:       30 * time.Second,
		Client:              es,
		Decoder:             nil,
		DebugLogger:         nil,
		OnError:             nil,
		OnFlushStart:        nil,
		OnFlushEnd:          nil,
		Index:               "test_bulk_index",
		ErrorTrace:          false,
		FilterPath:          nil,
		Header:              nil,
		Human:               false,
		Pipeline:            "",
		Pretty:              false,
		Refresh:             "",
		Routing:             "",
		Source:              nil,
		SourceExcludes:      nil,
		SourceIncludes:      nil,
		Timeout:             0,
		WaitForActiveShards: "",
	}

	indexer, _ := esutil.NewBulkIndexer(bic)
	start := time.Now().UTC()

	indexer.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			Action: "index",
			Body:   strings.NewReader(`{"title":"Test1"}`),
		})
	indexer.Add(
		context.Background(),
		esutil.BulkIndexerItem{
			Action: "index",
			Body:   strings.NewReader(`{"title":"Test2", "arr": [{"a":1,"b":"str"}]}`),
		})
	indexer.Close(context.Background())

	biStats := indexer.Stats()

	// Report the results: number of indexed docs, number of errors, duration, indexing rate
	//
	log.Println(strings.Repeat("▔", 65))

	dur := time.Since(start)

	if biStats.NumFailed > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			humanize.Comma(int64(biStats.NumFailed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	}
}

func TestOplogParse(t *testing.T) {
	oplogRecord := &oplog.ParsedLog{
		Timestamp: 1234567,
		Operation: "i",
		Namespace: "test.test1",
		Object: bson.D{
			bson.DocElem{
				Name:  "_id",
				Value: bson.ObjectIdHex("5feaedff8ae1b3bdad89801e"),
			},
			bson.DocElem{
				Name: "a",
				Value: 1,
			},
		},
		Query: bson.M{
			"s": 1,
		},
	}


	fmt.Println(oplogRecord)

	es, _ := elasticsearch.NewDefaultClient()
	//log.Println(elasticsearch.Version)
	//log.Println(es.Info())

	if oplogRecord.Operation == "i" {
		fmt.Println(oplogRecord.Object)
		var m bson.M
		m = bson.M{}

		var doc_id string
		for _, e := range oplogRecord.Object {
			if e.Name == "_id" {
				doc_id = e.Value.(bson.ObjectId).Hex()
				m["id"] = doc_id
			} else {
				m[e.Name] = e.Value
			}
		}

		buf1, err := bson.MarshalJSON(m)

		fmt.Println()
		fmt.Printf("%v", string(buf1))
		fmt.Println()




		//body := &buf
		body := strings.NewReader(string(buf1))
		req := esapi.IndexRequest{
			Index:               "mongoshake_es_test",
			DocumentType:        "doc_type",
			DocumentID:          doc_id,
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
		res, err := req.Do(context.Background(), es)
		log.Println(res)
		log.Println(err)

		assert.Equal(t, 1, res.StatusCode/200)
	}
}

func TestEsReplayer(t *testing.T) {

	var nr int
	{
		fmt.Printf("TestEsReplayer case %d.\n", nr)
		nr++

		data := &oplog.ParsedLog{
			Timestamp: 1234567,
			Operation: "o",
			Namespace: "a.b",
			Object: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: "xxx",
				},
			},
			Query: bson.M{
				"what": "fff",
			},
		}

		out, err := bson.Marshal(data)
		assert.Equal(t, nil, err, "should be equal")

		r := NewElasticSearchReplayer(0)
		ret := r.Sync(&tunnel.TMessage{
			RawLogs: [][]byte{out},
		}, nil)
		assert.Equal(t, int64(0), ret, "should be equal")

		time.Sleep(1 * time.Second)

		ret = r.Sync(&tunnel.TMessage{
			RawLogs: [][]byte{},
		}, nil)
		assert.Equal(t, int64(1234567), ret, "should be equal")
	}
}
