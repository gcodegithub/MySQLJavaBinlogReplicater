MySQLJavaBinlogReplicater
=========================

MySQLJavaBinlogReplicater


1.伪装MySQL从库，获取增量数据，导入到目标Mongodb中。
2.伪装Mongodb从库，获取增量数据，导入到目标Mongodb中。
3.借鉴了canal部分binlog格式解析代码
4.配置文件在conf目录中

ppm10103 AT qq.com


-----------------------------------

MySQL实时增量数据同步系统部署
工程目标
 解决海量数据MySQL查询缓慢，经常死锁等普遍问题
 解决MongoDB没有跨行事务问题
实现业务写MySQL，读MongodB，来实现业务事务完整，海量快速查询
MySQL到Mongodb增量数据同步功能实现
（该工程也支持从Mongodb到Mongodb的增量同步功能）

工程介绍
伪装MySQL从库，实时同步MySQL增量数据到MongoDB中间库中

MySQL数据库配置
server-id=1
log_bin=/var/log/mysql/mysql-bin.log
binlog-format=ROW
innodb_flush_log_at_trx_commit=1
sync_binlog=1
expire_logs_days=1095
max_binlog_size=1024M
lower_case_table_names = 0

MySQL数据库确保每个表都有唯一主键，否则无法进行一对一类型的同步
工程系统配置文件 conf/sysconfig.properties
#表明从MySQL取得增量数据 for mysql binlog source
manager.impclass=cn.ce.binlog.manager.MySQLSourceManager
#for mongo oplog source
#manager.impclass=cn.ce.binlog.manager.MongoDBSourceManager

#with CURD version,普通场景使用
consu.impclass=cn.ce.binlog.mongo.simple.MongoDBCURDSimpleImp
# For Init DB use,only insert，只有删除增加的切割数据场景使用
#consu.impclass=cn.ce.binlog.mongo.simple.MongoForCutImp

#the delete operation is use tomb mark or really del，中间库是否保留MySQL表中删除的数据
consu.ismark=true

#mark this project id，伪装的MySQL从库ID号，必须全局唯一
bootstrap.slaveid=10103


#all mongodb used，中间库使用
bootstrap.mongo.connectionsPerHost=500
bootstrap.mongo.threadsAllowedToBlockForConnectionMultiplier=5

#target mongodb，中间mongodb库的配置
bootstrap.mongo.seeds=192.168.24.1
bootstrap.mongo.port=27017
bootstrap.mongo.user=
bootstrap.mongo.pass=
bootstrap.mongo.forcedbname=DVS_MIDDLE_DB


#meta file dir，元信息存储目录
bootstrap.mysql.vo.filepool.dir=/Users/akui/vobinlogfiles/

#检查点存储目录
binlogparse.checkpoint.fullpath.file=/Users/akui/vobinlogfiles/binlogPosClintIdMap.properties

#ojbect seriliaziable dir，序列化文件存储文件
binlogpares.eventseri.dir=/Users/akui/vobinlogfiles/

#当源库是Mongo时的检查点文件
oplogparse.checkpoint.fullpath.file=/Users/akui/vobinlogfiles/oplogPosClintIdMap.properties



#the source mysql，MySQL源配置
bootstrap.mysql.master.ip=192.168.24.1
bootstrap.mysql.master.port=3306
bootstrap.mysql.master.user=canal
bootstrap.mysql.master.pass=canal

#the source mongodb，当源为Mongodb的配置
# rs type
bootstrap.source.mongo.monitortb=oplog.rs
# master type
#bootstrap.source.mongo.monitortb=oplog.$main
bootstrap.source.mongo.seeds=192.168.24.1
bootstrap.source.mongo.port=27018
bootstrap.source.mongo.user=
bootstrap.source.mongo.pass=


#---------------------------------ALARM EMAIL
all.alarm.smtpprot=25
all.alarm.smtpadd=smtp.300.cn
all.alarm.send.email.add=xxxx@300.cn
all.alarm.send.email.pass=xxxx
all.alarm.rec.email.add=wj@300.cn



binlogPosClintIdMap.properties
#34121为sysconfig.properties中bootstrap.mysql.master.slaveid的值
# value需要在MySQL数据库中通过show master status命令查询获取
34121.binlogPosition=207201049
34121.filenameKey=mysql-bin.000015

参考示例：
数据查询：

配置文件：
34121.binlogPosition=107
34121.filenameKey=mysql-bin.000006

MongoDB索引
MySQL数据同步到Mongodb后，MySQL表的主键在Mongodb中自动建立对应MongoDB索引


Mongodb特殊追加字段	说明
dvs_server_ts	Mongodb中间库时间戳，类型Timestamp
dvs_client_rec	写入MOngodb，类型毫秒数
dvs_thread_code	数值随机在1-100之间，包含边界
dvs_mysql_op_type	数据操作类型，UPDATE，INSERT，DELETE
when   	业务数据发生时间，比如业务系统写MySQL记录时间，类型秒数


