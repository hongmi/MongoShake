package utils

import (
	"fmt"
	"github.com/vinllen/mgo/bson"
	son "go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"reflect"
	"time"
)

var tOID = reflect.TypeOf(bson.ObjectIdHex("5feaedff8ae1b3bdad89801e"))
var tDateTime = reflect.TypeOf(time.Time{})
var tOrderKey = reflect.TypeOf(bson.MinKey)
var tUndefined = reflect.TypeOf(bson.Undefined)

////  "2006-01-02 15:04:05.999999999 -0700 MST"
func dateTimeEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	const jDateFormat = "2006-01-02T15:04:05.999Z"
	if !val.IsValid() || val.Type() != tDateTime {
		return bsoncodec.ValueEncoderError{Name: "DateTimeEncodeValue", Types: []reflect.Type{tDateTime}, Received: val}
	}

	t := val.Interface().(time.Time)

	return vw.WriteString(t.Format(jDateFormat))
}

func objectIDEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tOID {
		return bsoncodec.ValueEncoderError{Name: "ObjectIDEncodeValue", Types: []reflect.Type{tOID}, Received: val}
	}
	s := val.Interface().(bson.ObjectId).Hex()
	return vw.WriteString(s)
}

func orderKeyEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tOrderKey {
		return bsoncodec.ValueEncoderError{Name: "orderKeyEncodeValue", Types: []reflect.Type{tOrderKey}, Received: val}
	}

	switch val.Int() {
	case int64(bson.MinKey):
		return vw.WriteString(`$minKey`)
	case int64(bson.MaxKey):
		return vw.WriteString(`$maxKey`)
	}
	panic(fmt.Sprintf("invalid $minKey/$maxKey value: %+v", val))
}

func undefinedEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tUndefined {
		return bsoncodec.ValueEncoderError{Name: "undefinedEncodeValue", Types: []reflect.Type{tUndefined}, Received: val}
	}
	s := "$undefined"
	return vw.WriteString(s)
}

func CreateCustomRegistry() *bsoncodec.RegistryBuilder {
	var primitiveCodecs son.PrimitiveCodecs
	rb := bsoncodec.NewRegistryBuilder()
	bsoncodec.DefaultValueEncoders{}.RegisterDefaultEncoders(rb)
	bsoncodec.DefaultValueDecoders{}.RegisterDefaultDecoders(rb)
	rb.RegisterEncoder(tDateTime, bsoncodec.ValueEncoderFunc(dateTimeEncodeValue))
	rb.RegisterEncoder(tOID, bsoncodec.ValueEncoderFunc(objectIDEncodeValue))
	rb.RegisterEncoder(tOrderKey, bsoncodec.ValueEncoderFunc(orderKeyEncodeValue))
	rb.RegisterEncoder(tUndefined, bsoncodec.ValueEncoderFunc(undefinedEncodeValue))
	primitiveCodecs.RegisterPrimitiveCodecs(rb)
	return rb
}

func MarshalExtJSONWithRegistry(val interface{}) ([]byte, error) {
	var customRegistry = CreateCustomRegistry().Build()
	j, err := son.MarshalExtJSONWithRegistry(customRegistry, val, false, false)
	return j, err
}



