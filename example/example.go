package main

import (
	"fmt"
	"log"
	"time"

	"github.com/baidu/mochow-sdk-go/client"
	"github.com/baidu/mochow-sdk-go/mochow"
	"github.com/baidu/mochow-sdk-go/mochow/api"
)

type MochowTest struct {
	database string
	table    string
	client   *mochow.Client
}

func NewMochowTest(database, table string, config *mochow.ClientConfiguration) (*MochowTest, error) {
	client, err := mochow.NewClientWithConfig(config)
	if err != nil {
		return nil, err
	}
	mochowTest := &MochowTest{database: database, table: table, client: client}
	return mochowTest, nil
}

func (m *MochowTest) clearEnv() error {
	// skip drop database and table when not existed
	dbExisted, err := m.client.HasDatabase(m.database)
	if err != nil {
		log.Fatalf("Fail to check database due to error: %v", err)
		return err
	}
	if !dbExisted {
		return nil
	}

	// drop table first if existed
	tableExisted, err := m.client.HasTable(m.database, m.table)
	if err != nil {
		log.Fatalf("Fail to check table due to error: %v", err)
		return err
	}
	if tableExisted {
		if err := m.client.DropTable(m.database, m.table); err != nil {
			log.Printf("Fail to drop table due to error: %v", err)
			return err
		}
		// wait util drop finished
		for {
			time.Sleep(time.Second * 5)
			if _, err := m.client.DescTable(m.database, m.table); err != nil {
				if realErr, ok := err.(*client.BceServiceError); ok {
					if realErr.StatusCode == 404 || realErr.Code == int(api.TableNotExist) {
						log.Println("Table already dropped")
						break
					}
				}
			}
		}
	}

	// drop database if existed
	if dbExisted {
		log.Printf("Try to drop existed database: %s", m.database)
		if err := m.client.DropDatabase(m.database); err != nil {
			log.Fatalf("Fail to drop database due to error: %v", err)
			return err
		}
	}

	return nil
}

func (m *MochowTest) createDatabaseAndTable() error {
	var err error

	// create database
	err = m.client.CreateDatabase(m.database)
	if err != nil {
		log.Fatalf("Fail to create database due to error: %v", err)
		return err
	}

	// Fields
	fields := []api.FieldSchema{
		{
			FieldName:     "id",
			FieldType:     api.FieldTypeString,
			PrimaryKey:    true,
			PartitionKey:  true,
			AutoIncrement: false,
			NotNull:       true,
		},
		{
			FieldName: "bookName",
			FieldType: api.FieldTypeString,
			NotNull:   true,
		},
		{
			FieldName: "author",
			FieldType: api.FieldTypeString,
			NotNull:   true,
		},
		{
			FieldName: "page",
			FieldType: api.FieldTypeUint32,
			NotNull:   true,
		},
		{
			FieldName: "vector",
			FieldType: api.FieldTypeFloatVector,
			NotNull:   true,
			Dimension: 3,
		},
		{
			FieldName: "segment",
			FieldType: "TEXT",
			NotNull:   true,
		},
	}

	// Indexes
	autoBuildPolicy := api.NewAutoBuildIncrementPolicy()
	autoBuildPolicy.AddRowCountIncrement(5000)
	indexes := []api.IndexSchema{
		{
			IndexName: "book_name_idx",
			Field:     "bookName",
			IndexType: api.SecondaryIndex,
		},
		{
			IndexName:  "vector_idx",
			Field:      "vector",
			IndexType:  api.HNSW,
			MetricType: api.L2,
			Params: api.VectorIndexParams{
				"M":              32,
				"efConstruction": 200,
			},
			AutoBuild:       true,
			AutoBuildPolicy: autoBuildPolicy.Params(),
		},
		{
			IndexName:       "book_segment_inverted_idx",
			IndexType:       api.InvertedIndex,
			Fields:          []string{"segment"},
			FieldAttributes: []api.InvertedIndexFieldAttribute{api.Analyzed},
			Params: api.InvertedIndexParams{
				"analyzer":  api.ChineseAnalyzer,
				"parseMode": api.FineMode,
			},
		},
	}

	// create table
	createTableArgs := &api.CreateTableArgs{
		Database:    m.database,
		Table:       m.table,
		Description: "basic test",
		Replication: 3,
		Partition: &api.PartitionParams{
			PartitionType: api.HASH,
			PartitionNum:  3,
		},
		EnableDynamicField: false,
		Schema: &api.TableSchema{
			Fields:  fields,
			Indexes: indexes,
		},
	}
	if err := m.client.CreateTable(createTableArgs); err != nil {
		log.Fatalf("Fail to create table due to error: %v", err)
		return err
	}
	for {
		time.Sleep(5)
		describeTableResult, err := m.client.DescTable(m.database, m.table)
		if err == nil && describeTableResult.Table.State == api.TableStateNormal {
			log.Println("Table create finished")
			break
		}
	}
	return nil
}

func (m *MochowTest) upsertData() error {
	data := []api.Row{
		{
			Fields: map[string]interface{}{
				"id":       "0001",
				"bookName": "西游记",
				"author":   "吴承恩",
				"page":     21,
				"vector":   []float32{0.2123, 0.21, 0.213},
				"segment":  "富贵功名，前缘分定，为人切莫欺心。",
			},
		},
		{
			Fields: map[string]interface{}{
				"id":       "0002",
				"bookName": "西游记",
				"author":   "吴承恩",
				"page":     22,
				"vector":   []float32{0.2123, 0.22, 0.213},
				"segment":  "正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。",
			},
		},
		{
			Fields: map[string]interface{}{
				"id":       "0003",
				"bookName": "三国演义",
				"author":   "罗贯中",
				"page":     23,
				"vector":   []float32{0.2123, 0.23, 0.213},
				"segment":  "细作探知这个消息，飞报吕布。",
			},
		},
		{
			Fields: map[string]interface{}{
				"id":       "0004",
				"bookName": "三国演义",
				"author":   "罗贯中",
				"page":     24,
				"vector":   []float32{0.2123, 0.24, 0.213},
				"segment":  "布大惊，与陈宫商议。宫曰：“闻刘玄德新领徐州，可往投之。” 布从其言，竟投徐州来。有人报知玄德。",
			},
		},
		{
			Fields: map[string]interface{}{
				"id":       "0005",
				"bookName": "三国演义",
				"author":   "罗贯中",
				"page":     25,
				"vector":   []float32{0.2123, 0.24, 0.213},
				"segment":  "玄德曰：“布乃当今英勇之士，可出迎之。”糜竺曰：“吕布乃虎狼之徒，不可收留；收则伤人矣。",
			},
		},
	}
	upsertArgs := &api.UpsertRowArg{
		Database: m.database,
		Table:    m.table,
		Rows:     data,
	}
	upsertResult, err := m.client.UpsertRow(upsertArgs)
	if err != nil {
		log.Fatalf("Fail to upsert row due to error: %v", err)
		return err
	}
	log.Printf("Upsert row result: %+v", upsertResult)
	return nil
}

func (m *MochowTest) queryData() error {
	queryArgs := &api.QueryRowArgs{
		Database: m.database,
		Table:    m.table,
		PrimaryKey: map[string]interface{}{
			"id": "0001",
		},
		Projections:    []string{"id", "bookName"},
		RetrieveVector: false,
	}
	queryResult, err := m.client.QueryRow(queryArgs)
	if err != nil {
		log.Fatalf("Fail to query row due to error: %v", err)
		return err
	}
	log.Printf("Query row result: %+v", queryResult)
	return nil
}

func (m *MochowTest) selectData() error {
	selectArgs := &api.SelectRowArgs{
		Database:    m.database,
		Table:       m.table,
		Projections: []string{"id", "bookName"},
		Limit:       1,
	}
	for {
		selectResult, err := m.client.SelectRow(selectArgs)
		if err != nil {
			log.Fatalf("Fail to select row due to error: %v", err)
			return err
		}
		log.Printf("Select row result: %+v", selectResult)
		if !selectResult.IsTruncated {
			break
		}
		selectArgs.Marker = selectResult.NextMarker
	}
	return nil
}

func (m *MochowTest) updateData() error {
	updateArgs := &api.UpdateRowArgs{
		Database: m.database,
		Table:    m.table,
		PrimaryKey: map[string]interface{}{
			"id": "0001",
		},
		Update: map[string]interface{}{
			"bookName": "红楼梦",
			"author":   "曹雪芹",
			"page":     100,
			"segment":  "满纸荒唐言，一把辛酸泪",
		},
	}
	err := m.client.UpdateRow(updateArgs)
	if err != nil {
		log.Fatalf("Fail to update row due to error: %v", err)
		return err
	}
	return nil
}

func (m *MochowTest) topkSearch() error {
	// rebuild vector index
	if err := m.client.RebuildIndex(m.database, m.table, "vector_idx"); err != nil {
		log.Fatalf("Fail to rebuild index due to error: %v", err)
		return err
	}
	for {
		time.Sleep(5)
		descIndexResult, _ := m.client.DescIndex(m.database, m.table, "vector_idx")
		if descIndexResult.Index.State == api.IndexStateNormal {
			log.Println("Index rebuild finished")
			break
		}
	}

	// search
	vector := api.FloatVector([]float32{0.3123, 0.43, 0.213})

	searchArgs := &api.VectorSearchArgs{
		Database: m.database,
		Table:    m.table,
		Request: api.VectorTopkSearchRequest{}.
			New("vector", vector, 5).
			Filter("bookName='三国演义'").
			Config(api.VectorSearchConfig{}.New().Ef(200)),
	}

	searchResult, err := m.client.VectorSearch(searchArgs)
	if err != nil {
		log.Fatalf("Fail to search row due to error: %v", err)
		return err
	}

	log.Printf("topk search result: %+v", searchResult.Rows)
	return nil
}

func (m *MochowTest) rangeSearch() error {
	vector := api.FloatVector([]float32{0.3123, 0.43, 0.213})

	searchArgs := &api.VectorSearchArgs{
		Database: m.database,
		Table:    m.table,
		Request: api.VectorRangeSearchRequest{}.
			New("vector", vector, api.DistanceRange{Min: 0, Max: 20}).
			Filter("bookName='三国演义'").
			Limit(15).
			Config(api.VectorSearchConfig{}.New().Ef(200)),
	}

	searchResult, err := m.client.VectorSearch(searchArgs)
	if err != nil {
		log.Fatalf("Fail to search row due to error: %v", err)
		return err
	}

	log.Printf("range search result: %+v", searchResult.Rows)
	return nil
}

func (m *MochowTest) batchSearch() error {
	vectors := []api.Vector{
		api.FloatVector{0.3123, 0.43, 0.213},
		api.FloatVector{0.5512, 0.33, 0.43},
	}

	searchArgs := &api.VectorSearchArgs{
		Database: m.database,
		Table:    m.table,
		Request: api.VectorBatchSearchRequest{}.New("vector", vectors).
			Filter("bookName='三国演义'").
			Limit(10).
			Config(api.VectorSearchConfig{}.New().Ef(200)).
			Projections([]string{"id", "bookName", "author", "page"}),
	}

	searchResult, err := m.client.VectorSearch(searchArgs)
	if err != nil {
		log.Fatalf("Fail to batch search row due to error: %v", err)
		return err
	}

	log.Printf("batch search result: %+v", searchResult.BatchRows)

	return nil
}

func (m *MochowTest) bm25Search() error {
	searchArgs := &api.BM25SearchArgs{
		Database: m.database,
		Table:    m.table,
		Request: api.BM25SearchRequest{}.
			New("book_segment_inverted_idx", "吕布").
			Limit(12).
			Filter("bookName='三国演义'").
			ReadConsistency("STRONG").
			Projections([]string{"id", "vector"}),
	}

	searchResult, err := m.client.BM25Search(searchArgs)
	if err != nil {
		log.Fatalf("Fail to search row due to error: %v", err)
		return err
	}

	log.Printf("bm25 search result: %+v", searchResult.Rows)
	return nil
}

func (m *MochowTest) hybridSearch() error {
	vector := api.FloatVector([]float32{0.3123, 0.43, 0.213})

	request := api.HybridSearchRequest{}.
		New(
			api.VectorTopkSearchRequest{}.New("vector", vector, 15).Config(api.VectorSearchConfig{}.New().Ef(200)), /*vector search args*/
			api.BM25SearchRequest{}.New("book_segment_inverted_idx", "吕布"),                                         /*BM25 search args*/
			0.4, /*vector search weight*/
			0.6 /*BM25 search weight*/).
		Filter("bookName='三国演义'").
		Limit(15)

	searchArgs := &api.HybridSearchArgs{
		Database: m.database,
		Table:    m.table,
		Request:  request,
	}

	searchResult, err := m.client.HybridSearch(searchArgs)
	if err != nil {
		log.Fatalf("Fail to search row due to error: %v", err)
		return err
	}

	log.Printf("hybrid search result: %+v", searchResult.Rows)
	return nil
}

func (m *MochowTest) deleteDataWithPK() error {
	deleteArgs := &api.DeleteRowArgs{
		Database: m.database,
		Table:    m.table,
		PrimaryKey: map[string]interface{}{
			"id": "0001",
		},
	}
	if err := m.client.DeleteRow(deleteArgs); err != nil {
		log.Fatalf("Fail to delete row due to error: %v", err)
		return err
	}
	return nil
}

func (m *MochowTest) deleteDataWithFilter() error {
	deleteArgs := &api.DeleteRowArgs{
		Database: m.database,
		Table:    m.table,
		Filter:   "id = '0002'",
	}
	if err := m.client.DeleteRow(deleteArgs); err != nil {
		log.Fatalf("Fail to delete row due to error: %v", err)
		return err
	}
	return nil
}

func (m *MochowTest) dropAndCreateVIndex() error {
	// drop vindex
	if err := m.client.DropIndex(m.database, m.table, "vector_idx"); err != nil {
		log.Fatalf("Fail to drop index due to error: %v", err)
		return err
	}
	for {
		time.Sleep(5)
		_, err := m.client.DescIndex(m.database, m.table, "vector_idx")
		if realErr, ok := err.(*client.BceServiceError); ok {
			if realErr.Code == int(api.IndexNotExist) {
				log.Print("Index already dropped")
				break
			}
		}
	}

	// create vector index
	createIndexArgs := &api.CreateIndexArgs{
		Database: m.database,
		Table:    m.table,
		Indexes: []api.IndexSchema{
			{
				IndexName:  "vector_idx",
				Field:      "vector",
				IndexType:  api.HNSW,
				MetricType: api.L2,
				Params: api.VectorIndexParams{
					"M":              16,
					"efConstruction": 200,
				},
			},
		},
	}
	if err := m.client.CreateIndex(createIndexArgs); err != nil {
		log.Fatalf("Fail to create index due to error: %v", err)
		return err
	}
	return nil
}

func main() {
	// Init client
	clientConfig := &mochow.ClientConfiguration{
		Account:  "root",
		APIKey:   "*********",
		Endpoint: "http://*.*.*.*:*", // example:http://127.0.0.1:8511
	}
	mochowTest, err := NewMochowTest("book", "book_segments", clientConfig)
	if err != nil {
		log.Fatalf("Fail to init mochow example, err:%v", err)
	}
	log.Println("Init MochowTest success.")

	if err := mochowTest.clearEnv(); err != nil {
		fmt.Printf("Fail to clear env, err:%v", err)
		return
	}
	log.Println("Clear Env success.")

	// Create database and table
	if err := mochowTest.createDatabaseAndTable(); err != nil {
		log.Printf("Fail to create database and table, err:%v", err)
		return
	}
	log.Println("Create database and table success.")

	// Upsert data
	if err := mochowTest.upsertData(); err != nil {
		log.Printf("Fail to upsert row, err:%v", err)
		return
	}
	log.Println("Upsert row success.")

	// Query data
	if err := mochowTest.queryData(); err != nil {
		log.Printf("Fail to query row, err:%v", err)
		return
	}
	log.Println("Query row success.")

	// Select data
	if err := mochowTest.selectData(); err != nil {
		log.Printf("Fail to select row, err:%v", err)
		return
	}
	log.Println("Select row success.")

	// Update data
	if err := mochowTest.updateData(); err != nil {
		log.Printf("Fail to update row, err:%v", err)
		return
	}
	log.Println("Update row success.")

	// Search data
	if err := mochowTest.topkSearch(); err != nil {
		log.Printf("Fail to topk search, err:%v", err)
		return
	}
	if err := mochowTest.rangeSearch(); err != nil {
		log.Printf("Fail to range search, err:%v", err)
		return
	}
	if err := mochowTest.batchSearch(); err != nil {
		log.Printf("Fail to batch search, err:%v", err)
		return
	}
	if err := mochowTest.bm25Search(); err != nil {
		log.Printf("Fail to bm25 search, err:%v", err)
		return
	}
	if err := mochowTest.hybridSearch(); err != nil {
		log.Printf("Fail to hybrid search, err:%v", err)
		return
	}
	log.Println("Search row success.")

	// Delete data with pk
	if err := mochowTest.deleteDataWithPK(); err != nil {
		log.Printf("Fail to delete row with pk, err:%v", err)
		return
	}
	log.Println("Delete row with pk success.")

	// Delete data with filter
	if err := mochowTest.deleteDataWithFilter(); err != nil {
		log.Printf("Fail to delete row with filter, err:%v", err)
		return
	}
	log.Println("Delete row with filter success.")

	// drop and recreate vindex
	if err := mochowTest.dropAndCreateVIndex(); err != nil {
		log.Printf("Fail to drop and recreate vindex, err:%v", err)
		return
	}
	log.Println("Drop and recreate vindex success.")

	// clear env
	if err := mochowTest.clearEnv(); err != nil {
		fmt.Printf("Fail to clear env, err:%v", err)
		return
	}
	log.Println("Clear Env success")
}
