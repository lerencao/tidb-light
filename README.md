### TiDB SQL2KV

基本实现思路：


```go
func main() {
	cfg := newConfig()

	pdPath := cfg.StoreCfg.Path

	store, err := tikv.Driver{}.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdPath))
	if err != nil {
		log.Fatal(err)
	}

	// TODO: get this value from tidb
	actualDbId := int64(0)
	actualTableId := int64(0)
	initialBaseId := int64(1000)
	// TODO: get table ddl from tidb
	tableDDL := "create table"

	// 1. 获取 fake 的 id 生成器
	idAllocator := kvenc.NewAllocator()
	idAllocator.Reset(initialBaseId + 1)

	// 2. 创建 kvencoder
	kvencoder, err := kvenc.New("test", idAllocator)
	if err != nil {
		log.Fatal(err)
	}

	// 3. 给 fake 的 tidb session 填充表结构
	err = kvencoder.ExecDDLSQL(tableDDL)
	if err != nil {
		log.Fatal(err)
	}

	// 4. 构建 kvpair
	sql := "insert into xxx"
	kvPairs, affectedRows, err := kvencoder.Encode(sql, actualTableId)

	//5. 获取 ts
	version, err := store.CurrentVersion()
	commitTs := version.Ver

	// 6. TODO: send to tikv-importer
	openEngineRequest := import_kvpb.OpenEngineRequest{
		Uuid: []byte("abc"),
	}

	import_kvpb.NewImportKVClient(nil).OpenEngine(context.Background())
	// Save cur id value to actual storage
	// TODO: how to use store
	realAllocator := autoid.NewAllocator(store, actualDbId)

	realAllocator.Rebase(actualTableId, idAllocator.End()+1, false)

```
