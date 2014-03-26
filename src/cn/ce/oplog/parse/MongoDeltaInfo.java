package cn.ce.oplog.parse;

import java.net.SocketException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.BSONTimestamp;

import cn.ce.binlog.manager.Context;
import cn.ce.binlog.manager.MySQLSourceManager;
import cn.ce.binlog.manager.ProcessInfo;
import cn.ce.binlog.mongo.simple.MongoConnectionFactory;
import cn.ce.cons.Const;
import cn.ce.utils.mail.Alarm;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class MongoDeltaInfo {
	private final static Log logger = LogFactory.getLog(MongoDeltaInfo.class);

	private int noLoadcout = 0;
	private long noLoadMill = 0;
	private long noLoadBeginTimePoint = 0;

	private void sleepCurrThread() throws InterruptedException {
		noLoadcout++;
		if (noLoadcout == 1) {
			noLoadBeginTimePoint = System.currentTimeMillis();
		}
		if (noLoadcout > 1) {
			noLoadMill = System.currentTimeMillis() - noLoadBeginTimePoint;
		}
		if (noLoadMill > 30 * 1000 && noLoadMill < 10 * 60 * 1000) {
			Thread.sleep(50);
		} else if (noLoadMill > 10 * 60 * 1000) {
			Thread.sleep(200);
		}
	}

	private void updateContextCheckPoint(final DBObject dbObject,
			final Context context) {
		BSONTimestamp tsObj = (BSONTimestamp) dbObject.get("ts");
		Integer ts = tsObj.getTime();
		Integer inc = tsObj.getInc();
		if (ts == null || inc == null) {
			return;
		}
		context.setOplogtsInt(ts);
		context.setOplogincInt(inc);
	}

	/*
	 * { "ts" : { "t" : 1294582141000, "i" : 11 }, "op" : "i", "ns" :
	 * "mixi_top_city.building_77", "o" : { "_id" : "8577977_215_44_38", "uid" :
	 * "8577977", "x" : 44, "y" : 38, "pos" : 1, "btime" : 1293955498, "ntime" :
	 * 1294486420, "bid" : 18, "extprop" : 0, "status" : 0, "ucid" : 215 } }
	 */
	public void deltaInfoGet(final Context context,
			final ProcessInfo processInfo, Object[] params) throws Exception {
		String ipcsv = context.getSourceMongoIpCSV();
		int port = context.getSourceMongoPort();
		String db = "local";
		// String tb = "oplog.$main";
		String tb = context.getMonitortb();
		String username = context.getSourceMongoUser();
		String passwd = context.getSourceMongoPass();
		String connectionsPerHost_s = context.getConnectionsPerHost_s();
		String threadsAllowedToBlockForConnectionMultiplier_s = context
				.getThreadsAllowedToBlockForConnectionMultiplier_s();
		DBCollection oplogCollection = MongoConnectionFactory.getMongoTBConn(
				ipcsv, port, db, tb, username, passwd, connectionsPerHost_s,
				threadsAllowedToBlockForConnectionMultiplier_s);
		context.setParseThread(Thread.currentThread());
		try {
			// oplog查询条件
			BasicDBObject query = new BasicDBObject();
			// 限制查询结果显示的字段
			BasicDBObject field = new BasicDBObject();
			field.append("h", false);
			field.append("v", false);
			DBObject dbObject = null;
			DBCursor cur = null;
			while (!context.isPrepareStop()) {
				try {
					// 查询条件为大于上次的检查点。
					int ts = context.getOplogtsInt();
					int inc = context.getOplogincInt();
					query.append("ts", new BasicDBObject("$gt",
							new BSONTimestamp(ts, inc)));
					query.append("op", new BasicDBObject("$ne", "n"));
					cur = oplogCollection.find(query, field);
					if (cur.count() == 0) {
						this.sleepCurrThread();
					}
					while (cur.hasNext()) {
						dbObject = (BasicDBObject) cur.next();
						String dbInfo = dbObject.get("ns").toString();
						if (!StringUtils.isBlank(dbInfo)) {
							context.addDbObjectQueue(dbObject);
							this.updateContextCheckPoint(dbObject, context);
							logger.info("取得的增量OPLOG数据:" + dbObject);
						}
						// 如果存在数据，则重新定义时间。
						noLoadcout = 0;
						noLoadMill = 0;
						noLoadBeginTimePoint = 0;
					}
				} catch (MongoException.Network ex) {
					String msg = ex.getMessage();
					ex.printStackTrace();
					System.err.println("源Mongodb连接断开，准备重新连接，err:" + msg);
					MongoConnectionFactory.close();
				} catch (SocketException ex) {
					String msg = ex.getMessage();
					ex.printStackTrace();
					System.err.println("目标Mongodb连接断开，准备重新连接，err:" + msg);
					MongoConnectionFactory.close();
				} catch (InterruptedException ex) {
					if (context.isParseThreadStop()) {
						return;
					}
				}
			}
		} catch (Throwable ex) {
			context.setPrepareStop(true);
			String err = ex.getMessage();
			ex.printStackTrace();
			err = "消费mongodb 增量工程停止，MongoDeltaInfo失败，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n"
					+ context.toString() + "\n" + processInfo.toString());
		} finally {
			context.setPrepareStop(true);
			context.setParseThreadStop(true);
			MongoConnectionFactory.close();
			System.out.println("----------MongoDeltaInfo end");
		}
	}
}
