// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	model2 "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/pkg/errors"
)

type avroBatchEncoderSuite struct {
	encoder *AvroEventBatchEncoder
}

var _ = check.Suite(&avroBatchEncoderSuite{})

func (s *avroBatchEncoderSuite) SetUpSuite(c *check.C) {
	startHTTPInterceptForTestingRegistry(c)

	keyManager, err := NewAvroSchemaManager(context.Background(), nil, "http://127.0.0.1:8081", "-key")
	c.Assert(err, check.IsNil)

	valueManager, err := NewAvroSchemaManager(context.Background(), nil, "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	s.encoder = &AvroEventBatchEncoder{
		valueSchemaManager:  valueManager,
		keySchemaManager:    keyManager,
		resultBuf:           make([]*MQMessage, 0, 4096),
		enableTiDBExtension: false,
		decimalHandlingMode: "precise",
	}
}

func (s *avroBatchEncoderSuite) TearDownSuite(c *check.C) {
	stopHTTPInterceptForTestingRegistry()
}

func setBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	types.SetBinChsClnFlag(ft)
	return ft
}

func setFlag(ft *types.FieldType, flag uint) *types.FieldType {
	types.SetTypeFlag(&ft.Flag, flag, true)
	return ft
}

func setElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.Elems = elems
	return ft
}

type avroTestColumnTuple struct {
	col            model.Column
	colInfo        rowcodec.ColInfo
	expectedSchema interface{}
	expectedData   interface{}
	expectedType   string
}

var avroTestColumns = []*avroTestColumnTuple{
	{
		model.Column{Name: "tiny", Value: int64(1), Type: mysql.TypeTiny},
		rowcodec.ColInfo{ID: 1, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTiny)},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "short", Value: int64(1), Type: mysql.TypeShort},
		rowcodec.ColInfo{ID: 2, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeShort)},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "int24", Value: int64(1), Type: mysql.TypeInt24},
		rowcodec.ColInfo{ID: 3, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeInt24)},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "long", Value: int64(1), Type: mysql.TypeLong},
		rowcodec.ColInfo{ID: 4, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "longlong", Value: int64(1), Type: mysql.TypeLonglong},
		rowcodec.ColInfo{ID: 5, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLonglong)},
		avroSchema{Type: "long", Parameters: map[string]string{"tidbType": "BIGINT"}},
		int64(1), "long",
	},
	{
		model.Column{Name: "tinyunsigned", Value: uint64(1), Type: mysql.TypeTiny, Flag: model.UnsignedFlag},
		rowcodec.ColInfo{ID: 6, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeTiny), uint(model.UnsignedFlag))},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT UNSIGNED"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "shortunsigned", Value: uint64(1), Type: mysql.TypeShort, Flag: model.UnsignedFlag},
		rowcodec.ColInfo{ID: 7, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeShort), uint(model.UnsignedFlag))},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT UNSIGNED"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "int24unsigned", Value: uint64(1), Type: mysql.TypeInt24, Flag: model.UnsignedFlag},
		rowcodec.ColInfo{ID: 8, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeInt24), uint(model.UnsignedFlag))},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "INT UNSIGNED"}},
		int32(1), "int",
	},
	{
		model.Column{Name: "longunsigned", Value: uint64(1), Type: mysql.TypeLong, Flag: model.UnsignedFlag},
		rowcodec.ColInfo{ID: 9, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeLong), uint(model.UnsignedFlag))},
		avroSchema{Type: "long", Parameters: map[string]string{"tidbType": "INT UNSIGNED"}},
		int64(1), "long",
	},
	{
		model.Column{Name: "longlongunsigned", Value: uint64(1), Type: mysql.TypeLonglong, Flag: model.UnsignedFlag},
		rowcodec.ColInfo{ID: 10, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeLonglong), uint(model.UnsignedFlag))},
		avroSchema{Type: "long", Parameters: map[string]string{"tidbType": "BIGINT UNSIGNED"}},
		int64(1), "long",
	},
	{
		model.Column{Name: "float", Value: float64(3.14), Type: mysql.TypeFloat},
		rowcodec.ColInfo{ID: 11, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeFloat)},
		avroSchema{Type: "double", Parameters: map[string]string{"tidbType": "FLOAT"}},
		float64(3.14), "double",
	},
	{
		model.Column{Name: "double", Value: float64(3.14), Type: mysql.TypeDouble},
		rowcodec.ColInfo{ID: 12, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDouble)},
		avroSchema{Type: "double", Parameters: map[string]string{"tidbType": "DOUBLE"}},
		float64(3.14), "double",
	},
	{
		model.Column{Name: "bit", Value: uint64(683), Type: mysql.TypeBit},
		rowcodec.ColInfo{ID: 13, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBit)},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BIT", "length": "1"}},
		[]byte("\x02\xab"), "bytes",
	},
	{
		model.Column{Name: "decimal", Value: "129012.1230000", Type: mysql.TypeNewDecimal},
		rowcodec.ColInfo{ID: 14, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeNewDecimal)},
		avroLogicalTypeSchema{
			avroSchema: avroSchema{
				Type:       "bytes",
				Parameters: map[string]string{"tidbType": "DECIMAL"},
			},
			LogicalType: "decimal",
			Precision:   10,
			Scale:       0,
		},
		big.NewRat(129012123, 1000), "bytes.decimal",
	},
	{
		model.Column{Name: "tinytext", Value: []byte("hello world"), Type: mysql.TypeTinyBlob},
		rowcodec.ColInfo{ID: 15, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "mediumtext", Value: []byte("hello world"), Type: mysql.TypeMediumBlob},
		rowcodec.ColInfo{ID: 16, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeMediumBlob)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "text", Value: []byte("hello world"), Type: mysql.TypeBlob},
		rowcodec.ColInfo{ID: 17, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "longtext", Value: []byte("hello world"), Type: mysql.TypeLongBlob},
		rowcodec.ColInfo{ID: 18, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLongBlob)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "varchar", Value: []byte("hello world"), Type: mysql.TypeVarchar},
		rowcodec.ColInfo{ID: 19, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarchar)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "varstring", Value: []byte("hello world"), Type: mysql.TypeVarString},
		rowcodec.ColInfo{ID: 20, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarString)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "string", Value: []byte("hello world"), Type: mysql.TypeString},
		rowcodec.ColInfo{ID: 21, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeString)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TEXT"}},
		"hello world", "string",
	},
	{
		model.Column{Name: "tinyblob", Value: []byte("hello world"), Type: mysql.TypeTinyBlob, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 22, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "mediumblob", Value: []byte("hello world"), Type: mysql.TypeMediumBlob, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 23, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "blob", Value: []byte("hello world"), Type: mysql.TypeBlob, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 24, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeBlob))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "longblob", Value: []byte("hello world"), Type: mysql.TypeLongBlob, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 25, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "varbinary", Value: []byte("hello world"), Type: mysql.TypeVarchar, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 26, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "varbinary1", Value: []byte("hello world"), Type: mysql.TypeVarString, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 27, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeVarString))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "binary", Value: []byte("hello world"), Type: mysql.TypeString, Flag: model.BinaryFlag},
		rowcodec.ColInfo{ID: 28, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeString))},
		avroSchema{Type: "bytes", Parameters: map[string]string{"tidbType": "BLOB"}},
		[]byte("hello world"), "bytes",
	},
	{
		model.Column{Name: "enum", Value: uint64(1), Type: mysql.TypeEnum},
		rowcodec.ColInfo{ID: 29, IsPKHandle: false, VirtualGenCol: false, Ft: setElems(types.NewFieldType(mysql.TypeEnum), []string{"a,", "b"})},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "ENUM", "allowed": "a\\,,b"}},
		"a,", "string",
	},
	{
		model.Column{Name: "set", Value: uint64(1), Type: mysql.TypeSet},
		rowcodec.ColInfo{ID: 30, IsPKHandle: false, VirtualGenCol: false, Ft: setElems(types.NewFieldType(mysql.TypeSet), []string{"a,", "b"})},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "SET", "allowed": "a\\,,b"}},
		"a,", "string",
	},
	{
		model.Column{Name: "json", Value: `{"key": "value"}`, Type: mysql.TypeJSON},
		rowcodec.ColInfo{ID: 31, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeJSON)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "JSON"}},
		`{"key": "value"}`, "string",
	},
	{
		model.Column{Name: "date", Value: "2000-01-01", Type: mysql.TypeDate},
		rowcodec.ColInfo{ID: 32, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDate)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "DATE"}},
		"2000-01-01", "string",
	},
	{
		model.Column{Name: "datetime", Value: "2015-12-20 23:58:58", Type: mysql.TypeDatetime},
		rowcodec.ColInfo{ID: 33, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDatetime)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "DATETIME"}},
		"2015-12-20 23:58:58", "string",
	},
	{
		model.Column{Name: "timestamp", Value: "1973-12-30 15:30:00", Type: mysql.TypeTimestamp},
		rowcodec.ColInfo{ID: 34, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTimestamp)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TIMESTAMP"}},
		"1973-12-30 15:30:00", "string",
	},
	{
		model.Column{Name: "time", Value: "23:59:59", Type: mysql.TypeDuration},
		rowcodec.ColInfo{ID: 35, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeDuration)},
		avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "TIME"}},
		"23:59:59", "string",
	},
	{
		model.Column{Name: "year", Value: int64(1970), Type: mysql.TypeYear},
		rowcodec.ColInfo{ID: 36, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeYear)},
		avroSchema{Type: "int", Parameters: map[string]string{"tidbType": "YEAR"}},
		int64(1970), "int",
	},
}

func (s *avroBatchEncoderSuite) TestColumnToAvroSchema(c *check.C) {
	defer testleak.AfterTest(c)()

	for _, v := range avroTestColumns {
		schema, err := columnToAvroSchema(&v.col, v.colInfo.Ft, "precise")
		c.Assert(err, check.IsNil)
		c.Assert(schema, check.DeepEquals, v.expectedSchema)
		if v.col.Name == "decimal" {
			schema, err := columnToAvroSchema(&v.col, v.colInfo.Ft, "string")
			c.Assert(err, check.IsNil)
			c.Assert(schema, check.DeepEquals, avroSchema{Type: "string", Parameters: map[string]string{"tidbType": "DECIMAL"}})
		}
	}
}

func (s *avroBatchEncoderSuite) TestColumnToAvroData(c *check.C) {
	defer testleak.AfterTest(c)()

	for _, v := range avroTestColumns {
		data, str, err := columnToAvroData(&v.col, v.colInfo.Ft, "precise")
		c.Assert(err, check.IsNil)
		c.Assert(data, check.DeepEquals, v.expectedData)
		c.Assert(str, check.Equals, v.expectedType)
		if v.col.Name == "decimal" {
			data, str, err := columnToAvroData(&v.col, v.colInfo.Ft, "string")
			c.Assert(err, check.IsNil)
			c.Assert(data, check.Equals, "129012.1230000")
			c.Assert(str, check.Equals, "string")
		}
	}
}

func indentJSON(j string) string {
	var buf bytes.Buffer
	_ = json.Indent(&buf, []byte(j), "", "  ")
	return buf.String()
}

func (s *avroBatchEncoderSuite) TestRowToAvroSchema(c *check.C) {
	defer testleak.AfterTest(c)()

	table := model.TableName{
		Schema: "testdb",
		Table:  "rowtoavroschema",
	}
	fqdn := table.Schema + "." + table.Table
	var cols []*model.Column = make([]*model.Column, 0)
	var colInfos []rowcodec.ColInfo = make([]rowcodec.ColInfo, 0)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)
		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()
		cols = append(cols, &colNew)
		colInfos = append(colInfos, v.colInfo)
	}

	schema, err := rowToAvroSchema(fqdn, cols, colInfos, false, "precise")
	c.Assert(err, check.IsNil)
	c.Assert(indentJSON(schema), check.Equals, expectedSchemaWithoutExtension)
	_, err = goavro.NewCodec(schema)
	c.Assert(err, check.IsNil)

	schema, err = rowToAvroSchema(fqdn, cols, colInfos, true, "precise")
	c.Assert(err, check.IsNil)
	c.Assert(indentJSON(schema), check.Equals, expectedSchemaWithExtension)
	_, err = goavro.NewCodec(schema)
	c.Assert(err, check.IsNil)
}

func (s *avroBatchEncoderSuite) TestRowToAvroData(c *check.C) {
	defer testleak.AfterTest(c)()

	var cols []*model.Column = make([]*model.Column, 0)
	var colInfos []rowcodec.ColInfo = make([]rowcodec.ColInfo, 0)

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)
		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()
		cols = append(cols, &colNew)
		colInfos = append(colInfos, v.colInfo)
	}

	data, err := rowToAvroData(cols, colInfos, 417318403368288260, "c", false, "precise")
	c.Assert(err, check.IsNil)
	_, exists := data["tidbCommitTs"]
	c.Assert(exists, check.IsFalse)
	_, exists = data["tidbOp"]
	c.Assert(exists, check.IsFalse)

	data, err = rowToAvroData(cols, colInfos, 417318403368288260, "c", true, "precise")
	c.Assert(err, check.IsNil)
	v, exists := data["tidbCommitTs"]
	c.Assert(exists, check.IsTrue)
	c.Assert(v.(int64), check.Equals, int64(417318403368288260))
	v, exists = data["tidbOp"]
	c.Assert(exists, check.IsTrue)
	c.Assert(v.(string), check.Equals, "c")

}

func (s *avroBatchEncoderSuite) TestAvroEncode(c *check.C) {
	defer testleak.AfterTest(c)()

	var cols []*model.Column = make([]*model.Column, 0)
	var colInfos []rowcodec.ColInfo = make([]rowcodec.ColInfo, 0)

	cols = append(cols, &model.Column{Name: "id", Value: int64(1), Type: mysql.TypeLong, Flag: model.HandleKeyFlag})
	colInfos = append(colInfos, rowcodec.ColInfo{ID: 1000, IsPKHandle: true, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)})

	for _, v := range avroTestColumns {
		cols = append(cols, &v.col)
		colInfos = append(colInfos, v.colInfo)
		colNew := v.col
		colNew.Name = colNew.Name + "nullable"
		colNew.Value = nil
		colNew.Flag.SetIsNullable()
		cols = append(cols, &colNew)
		colInfos = append(colInfos, v.colInfo)
	}

	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "testdb",
			Table:  "avroencode",
		},
		Columns:  cols,
		ColInfos: colInfos,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.encoder.enableTiDBExtension = true
	defer func() {
		s.encoder.enableTiDBExtension = false
	}()

	keyCols, keyColInfos := event.HandleKeyColInfos()
	keySchema, err := rowToAvroSchema("testdb.avroencode", keyCols, keyColInfos, false, "precise")
	c.Assert(err, check.IsNil)
	avroKeyCodec, err := goavro.NewCodec(keySchema)
	c.Assert(err, check.IsNil)

	r, err := s.encoder.avroEncode(ctx, event, true)
	c.Assert(err, check.IsNil)
	res, _, err := avroKeyCodec.NativeFromBinary(r.data)
	c.Assert(err, check.IsNil)
	c.Assert(res, check.NotNil)
	for k, _ := range res.(map[string]interface{}) {
		// key shall not include these fields
		if k == "tidbCommitTs" || k == "tidbOp" {
			c.Fail()
		}
	}

	valueSchema, err := rowToAvroSchema("testdb.avroencode", cols, colInfos, true, "precise")
	c.Assert(err, check.IsNil)
	avroValueCodec, err := goavro.NewCodec(valueSchema)
	c.Assert(err, check.IsNil)

	r, err = s.encoder.avroEncode(ctx, event, false)
	c.Assert(err, check.IsNil)
	res, _, err = avroValueCodec.NativeFromBinary(r.data)
	c.Assert(err, check.IsNil)
	c.Assert(res, check.NotNil)
	for k, v := range res.(map[string]interface{}) {
		if k == "tidbOp" {
			c.Assert(v.(string), check.Equals, "c")
		}
	}
}

func (s *avroBatchEncoderSuite) TestAvroEnvelope(c *check.C) {
	defer testleak.AfterTest(c)()

	avroCodec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "testdb.avroenvelope",
          "fields" : [
            {"name": "id", "type": "int", "default": 0}
          ]
        }`)

	c.Assert(err, check.IsNil)

	testNativeData := make(map[string]interface{})
	testNativeData["id"] = 7

	bin, err := avroCodec.BinaryFromNative(nil, testNativeData)
	c.Assert(err, check.IsNil)

	res := avroEncodeResult{
		data:       bin,
		registryID: 7,
	}

	evlp, err := res.toEnvelope()
	c.Assert(err, check.IsNil)

	c.Assert(evlp[0], check.Equals, magicByte)
	c.Assert(evlp[1:5], check.BytesEquals, []byte{0, 0, 0, 7})

	parsed, _, err := avroCodec.NativeFromBinary(evlp[5:])
	c.Assert(err, check.IsNil)
	c.Assert(parsed, check.NotNil)

	id, exists := parsed.(map[string]interface{})["id"]
	c.Assert(exists, check.IsTrue)
	c.Assert(id, check.Equals, int32(7))
}

func (s *avroBatchEncoderSuite) TestEncodeRowChangedAndDDLEvent(c *check.C) {
	defer testleak.AfterTest(c)()

	testCaseUpdate := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "person",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag, Value: int64(1)},
			{Name: "name", Type: mysql.TypeVarchar, Value: []byte("Bob")},
			{Name: "tiny", Type: mysql.TypeTiny, Value: int64(255)},
			{Name: "utiny", Type: mysql.TypeTiny, Flag: model.UnsignedFlag, Value: uint64(100)},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
		},
		ColInfos: []rowcodec.ColInfo{
			{ID: 1, IsPKHandle: true, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
			{ID: 2, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarchar)},
			{ID: 3, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTiny)},
			{ID: 4, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeTiny), uint(model.UnsignedFlag))},
			{ID: 5, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
		},
	}

	testCaseDdl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test", Table: "person",
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  model2.ActionCreateTable,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := puller.NewMockPullerManager(c, true)
	defer pm.TearDown()
	pm.MustExec(testCaseDdl.Query)
	ddlPlr := pm.CreatePuller(0, []regionspan.ComparableSpan{regionspan.ToComparableSpan(regionspan.GetDDLSpan())})
	go func() {
		err := ddlPlr.Run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled {
			c.Fail()
		}
	}()

	info := pm.GetTableInfo("test", "person")
	testCaseDdl.TableInfo = new(model.SimpleTableInfo)
	testCaseDdl.TableInfo.Schema = "test"
	testCaseDdl.TableInfo.Table = "person"
	testCaseDdl.TableInfo.ColumnInfo = make([]*model.ColumnInfo, len(info.Columns))
	for i, v := range info.Columns {
		testCaseDdl.TableInfo.ColumnInfo[i] = new(model.ColumnInfo)
		testCaseDdl.TableInfo.ColumnInfo[i].FromTiColumnInfo(v)
	}

	msg, err := s.encoder.EncodeDDLEvent(testCaseDdl)
	c.Assert(msg, check.IsNil)
	c.Assert(err, check.IsNil)

	err = s.encoder.AppendRowChangedEvent(testCaseUpdate)
	c.Assert(err, check.IsNil)
}
