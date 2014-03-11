MySQLJavaBinlogReplicater
=========================

MySQLJavaBinlogReplicater

0.部署非常简单，写好一个配置文件，开发调试直接tomcat war部署即可，启动更简单，tomcat启动即可
1.伪装MySQL从库，获取增量数据，导入到目标Mongodb中。
2.伪装Mongodb从库，获取增量数据，导入到目标Mongodb中。
3.借鉴了canal部分binlog格式解析代码
4.配置文件在conf目录中
5.详细部署文档参考 doc/MySQL binlog实时数据系统部署.doc （重要文档）

容易出现的错误
1.MySQL每个表必须有一个唯一主键
2.MySQL配置binlog-format=ROW

ppm10103 AT qq.com

