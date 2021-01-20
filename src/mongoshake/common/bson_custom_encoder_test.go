package utils

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/vinllen/mgo/bson"
	"testing"
	"time"
	"encoding/base64"
)

type orderKey int64

func TestAllInOne(t *testing.T) {

	d := bson.D{
		bson.DocElem{
			Name:  "_id",
			Value: bson.ObjectIdHex("5feaedff8ae1b3bdad89801e"),
		},
		bson.DocElem{
			Name: "dt",
			Value: time.Unix(0, 1000000),
		},
		bson.DocElem{
			Name: "byte_array",
			Value: []byte{1},
		},
		bson.DocElem{
			Name: "binary",
			Value: bson.Binary{0, []byte{1}},
		},
		bson.DocElem{
			Name: "mongoTimestamp",
			Value: bson.MongoTimestamp(0),
		},
		bson.DocElem{
			Name: "regex",
			Value: bson.RegEx{"abc", "i"},
		},
		bson.DocElem{
			Name: "int64",
			Value: int64(0),
		},
		bson.DocElem{
			Name: "int",
			Value: int(0),
		},
		bson.DocElem{
			Name: "orderkey",
			Value: bson.MinKey,
		},
		bson.DocElem{
			Name: "undefined",
			Value: bson.Undefined,
		},
		bson.DocElem{
			Name: "obj",
			Value: bson.D{
				bson.DocElem{
					Name:  "_id",
					Value: bson.ObjectIdHex("5feaedff8ae1b3bdad89801e"),
				},
				bson.DocElem{
					Name: "int",
					Value: int(0),
				},
			},
		},
		bson.DocElem{
			Name: "array",
			Value: []string{"a", "b"},
		},
	}

	j, err := MarshalExtJSONWithRegistry(BsonToMap(&d))
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
	fmt.Println(string(j))

	var m map[string]interface{}
	if err := json.Unmarshal(j, &m); err != nil {
		fmt.Printf("%+v\n", err)
	}

	fmt.Println(m)

	//_id
	assert.Equal(t, "5feaedff8ae1b3bdad89801e", m["_id"], "should be equal")
	//dt
	assert.Equal(t, "1970-01-01T08:00:00.001Z", m["dt"], "should be equal")
	//bytearray
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{1}),
					m["byte_array"].(map[string]interface{})["$binary"].(map[string]interface{})["base64"],
					"should be equal")
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte{1}),
					m["binary"].(map[string]interface{})["data"].(map[string]interface{})["$binary"].(map[string]interface{})["base64"],
					"should be equal")
	assert.Equal(t, float64(0), m["mongoTimestamp"], "should be equal")
	assert.Equal(t, "abc", m["regex"].(map[string]interface{})["pattern"], "should be equal")
	assert.Equal(t, "i", m["regex"].(map[string]interface{})["options"], "should be equal")
	assert.Equal(t, float64(0), m["int64"], "should be equal")
	assert.Equal(t, float64(0), m["int"], "should be equal")
	assert.Equal(t, "$minKey", m["orderkey"], "should be equal")
	assert.Equal(t, "$undefined", m["undefined"], "should be equal")
	assert.Equal(t, "5feaedff8ae1b3bdad89801e", m["obj"].(map[string]interface{})["_id"], "should be equal")
}
