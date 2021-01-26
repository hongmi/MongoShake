#mongoshakees
mongoshake + sync to elasticsearch

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
    结果：mongoshakees退出，by design

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
    测试数据：1000万条记录，每条记录包含5个字段，每条oplog约200字节
    worker并发：4
   


