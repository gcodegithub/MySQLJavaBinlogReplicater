MySQLJavaBinlogReplicater
=========================

MySQLJavaBinlogReplicater


伪装MySQL从库，将MySQL主库Binlog日志内容暴露为REST风格Webservice，从而可以适配各种其他异构数据库进行同步。
借鉴了canal部分代码

-------------------------CXF REST演示
slaveId首次注册
http://localhost:8080/MySQLJavaBinlogReplicater/webservice/binlog/getToken.json?slaveId=111
返回
{"TokenAuthRes":{"msgDetail":"first visit,generate new token.","newToken":1384219423587,"resCode":"NEW_TOKEN"}}

第二次

http://localhost:8080/MySQLJavaBinlogReplicater/webservice/binlog/getDump.xml?slaveId=111&tokenInput=1384221380644&binlogPosition=107&binlogfilename=mysql-bin.000001&serverPort=3306&serverhost=192.168.215.1&username=canal&password=canal

-------------------------JAVA演示
运行BinlogParser类
