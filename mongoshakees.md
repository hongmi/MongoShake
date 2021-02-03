#mongoshakees
mongoshake + sync to elasticsearch

## 背景
mongoshake简介
*  [中文架构介绍文档](https://yq.aliyun.com/articles/603329)
   
整体架构上并没有进行修改，只是增加了对elasticsearch的全量和增量同步，并且增加容器化构建。

##功能点列表
1. 支持全量同步
2. 支持增量同步，包括增删查改
3. 支持全同步(全量同步增量同步一起)
4. 各种数据类型正确映射，嵌套结构正常映射
5. 程序重启能够恢复同步，而不是从头再来。
6. 支持多个collection同时同步，配置灵活化
7. 兼容mongoshake已有功能
8. 支持同步到需要认证的es集群
9. 稳定性

## 功能测试案例
01. 准备一个集合mongoshakees.func_test_full，包含_id, name, age字段，10w条记录。
    进行全量同步，检查总数，并随机抽查10条记录进行对比。
    结果：通过
02. 准备一个集合mongoshakees.func_test_incr，包含_id, name, age字段，10条记录。
    启动同步进程，依次对4条不同记录进行增删查改，观察elasticsearch对应索引文档是否正确
    结果：通过
03. 准备一个集合mongoshakees.func_test_all，包含_id, name, age字段，10w条记录。
    启动同步进程，依次对4条不同记录进行增删查改，观察elasticsearch对应索引文档是否正确，总数是否正确。
    结果：通过
04. 准备一个集合mongoshakees.func_test_data_type，包含
    ObjectID, int, long, datetime, regex, array<int>, string, document, array<document>, array<string>, array<array<int>>
    几种类型，共2条记录
    启动同步进程，观察elasticsearch相应文档的数据类型是否正确
    结果：通过
05. 准备一个集合mongoshakees.func_test_checkpoint，包含_id, name, age字段，10条记录。
    启动全同步，稍等一分钟，使用脚本持续插入记录，关闭mongoshakees五分钟，重新启动mongoshakees，
    停止脚本，观察mongoshakees日志，待日志显示一分钟没有记录在同步时，对比源数据和目的数据是否一致。
    结果：通过
06. 准备两个集合mongoshakees.func_test_col_1, mongoshakeee.func_test_col_2，mongoshakeee.func_test_col_3，各10条数据。
    配置集合mongoshakeee.func_test_col_1, mongoshakeee.func_test_col_2进行全同步，启动程序，过两分钟，检查elasticsearch是否包含这两个集合，而没有第三个。
    结果：通过
07. 准备一个集合mongoshakees.func_test_compatible，10条记录，配置同步到另外一个mongodb，启动进程，进行增删查改，检查同步是否正确
    结果：通过
08. 准备一个集合mongoshakees.func_test_es_auth，10条记录，配置同步到一个开启认证的es，启动进程，进行增删查改，观察是否进行了正确同步。
    结果：通过
09. 稳定性测试。准备一个集合mongoshakees.func_test_long_time，10w条记录，开启同步，同时启动脚本进行增删查改，运行1小时后，观察效果。
    结果：通过
10. 稳定性测试。准备一个集合mongoshakees.func_test_es_restart，10w条记录，开启同步，同时启动脚本进行增删查改，过两分钟后，重启elasticsearch集群，观察效果。
    结果：不通过，数据丢失了一部分。

## 性能测试方案
1. 测试环境，mongodb, es均运行于docker环境，配置如下
   CPU：4 Core 2GHz
   Memory：4G
   OS：linux
   MongoDB version: 4.0.20
   Go version: 1.15.6
2. 测试案例
    对标官方案例https://github.com/alibaba/MongoShake/wiki/MongoShake-Performance-Document#case-3
    mongodb部署：单实例集群
    测试数据：持续产生1000万条记录，单个集合表，每条记录包含5个字段，每条oplog约200字节
    worker并发：4
   
3. 测试结果
   1000w文档，用了45分钟，QPS大约4000
   同等测试环境，mock tunnel的QPS大约30000
4. 结论
    QPS能达到4000，基本能够满足大多数同步场景了。
    受限于硬件配置，qps较低。如果提高硬件配置，可以遇见qps能够大幅提高。

## 重要配置
```
sync_mode = incr #同步模式，all表示全量+增量同步，full表示全量同步，incr表示增量同步。

mongo_urls = mongodb://root:root@mongo:27017  #源MongoDB连接串信息，逗号分隔同一个副本集内的结点，分号分隔分片sharding实例，免密模式

# 同步时如果目的库存在，是否先删除目的库再进行同步，true表示先删除再同步，false表示不删除。
# 如果需要提前创建elasticsearch索引，请设置为false
full_sync.collection_exist_drop = true

tunnel = direct2es
# 如果是elasticsearch，使用英文分号分割多个host
tunnel.address = http://127.0.0.1:9400
tunnel.message = raw
# 用户名和密码，有些tunnel不支持将用户名和密码写到地址url当中
tunnel.username = elastic
tunnel.password = elastic

# 黑白名单过滤，目前不支持正则，白名单表示通过的namespace，黑名单表示过滤的namespace，
# 不能同时指定。分号分割不同namespace，每个namespace可以是db，也可以是db.collection。
filter.namespace.black =
filter.namespace.white = mongoshakees.perf_test_1

# checkpoint存储的db的名字
checkpoint.storage.db = mongoshake
# checkpoint存储的表的名字，如果启动多个mongoshake拉取同一个源可以修改这个表名以防止冲突。
checkpoint.storage.collection = ckpt_default.perf.test.1

# 如果选择direct2es，必须使用change_stream
incr_sync.mongo_fetch_method = change_stream

# 更新文档后,只需要更新的字段则设为false,需要全部文档内容则设为true
# 只在mongo_fetch_method = change_stream 模式下生效，且性能有所下降
incr_sync.change_stream.watch_full_document = true
```
## 启动命令
```
## 如果想打印详细日志，请追加选项--verbose 
./collector -conf=conf/collector-perf-test-1.conf  
```

## 容器化
增加了容器化构建脚本和docker-compose示例
容器化构建脚本：Dockerfile
需要通过volume挂载配置文件

## 系统架构理解
整体架构上并没有进行修改，只是增加了对elasticsearch的全量和增量同步。
参考官方文档
*  [中文架构介绍文档](https://yq.aliyun.com/articles/603329)

## 限制
1. 由于必须使用change_stream，要求愿端mongodb的版本要不小于4.0.1。后续会尝试支持oplog模式，以便实现对4.0.1之前版本的支持。
2. 由于elasticsearch不支持删除字段，因此针对文档的更新操作，只会新增字段和修改字段。
3. elasticsearch的数组类型，要求元素必须是同样类型的。因此源mongodb中的数据不能包含元素类型不一致的数组。
4. elasticsearch目前仅支持7版本。后续会添加对多个版本的支持。

## 实现过程
1. 全量同步
2. 增量同步
3. 数据类型转换
   由于内部oplog携带的文档数据是github.com/vinllen/mgo/bson，而改代码库没有提供自定义序列化控制方式，我们通过mongodb官方
   驱动go.mongodb.org/mongo-driver/bson来实现自定义的bson文档序列化，从而构造适合写入elasticsearch的数据结构。
   参考mongoshake/common/bson_custom_encoder.go