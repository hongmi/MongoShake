package utils

import (
	"github.com/elastic/go-elasticsearch"
	"github.com/vinllen/mgo/bson"
	"log"
)

func TestElasticSearchConn(url []string, username string, password string) (*elasticsearch.Client, error) {
	es, _ := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:             url,
		Username:              username,
		Password:              password,
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
	_, err := es.Info()
	return es, err
}


func convertToJsonObj(bo interface{}) (jo interface{}) {
	switch t := bo.(type) {
	case bson.D:
		return BsonToMap(&t)
	default:
		return t
	}
}

func BsonToMap(pd *bson.D) (m bson.M) {
	d := *pd
	m = make(bson.M, len(d))
	for _, item := range d {
		switch t := item.Value.(type) {
		case []interface{}:
			var a []interface{}
			for _, i := range t {
				a = append(a, convertToJsonObj(i))
			}
			m[item.Name] = a
		case bson.D:
			m[item.Name] = BsonToMap(&t)
		default:
			m[item.Name] = item.Value

		}
	}
	return m
}