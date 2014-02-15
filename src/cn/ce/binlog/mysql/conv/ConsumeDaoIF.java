package cn.ce.binlog.mysql.conv;

import java.util.concurrent.CountDownLatch;

public interface ConsumeDaoIF {

	public void syncMySQL2DB(java.util.ArrayList list, CountDownLatch latch,
			Object[] params) throws Exception;

	public void syncMongo2DB(java.util.ArrayList list, CountDownLatch latch,
			Object[] params) throws Exception;

	public void closeDBCon();
}
