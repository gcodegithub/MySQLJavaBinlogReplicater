package cn.ce.binlog.mongo.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.manager.Context;
import cn.ce.binlog.mysql.conv.ConsumeDaoIF;
import cn.ce.binlog.mysql.event.ColumnInfoValue;
import cn.ce.binlog.vo.SQLRowVO;
import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.mail.Alarm;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongoDBCURDSimpleImp extends AbsMongoDao implements ConsumeDaoIF {
	private final static Log logger = LogFactory
			.getLog(MongoDBCURDSimpleImp.class);

	private Context ctx;

	public MongoDBCURDSimpleImp(Context ctx) {
		this.ctx = ctx;
	}

	public void syncMySQL2DB(final java.util.ArrayList list,
			CountDownLatch latch, Object[] params) throws Exception {
		try {
			this.normalSync2Mongodb(list, ctx, params);
		} catch (Exception ex) {
			ctx.setPrepareStop(true);
			String err = ex.getMessage();
			ex.printStackTrace();
			err = "消费binlog工程停止，RowInfo失败，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n");
		} finally {
			if (latch != null) {
				latch.countDown();
			}
		}
	}

	public void syncMongo2DB(ArrayList list, CountDownLatch latch,
			Object[] params) throws Exception {
		try {
			this.normalSync2Mongodb(list, ctx, params);
		} catch (Exception ex) {
			ctx.setPrepareStop(true);
			String err = ex.getMessage();
			ex.printStackTrace();
			err = "消费oplog工程停止，RowInfo失败，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n");
		} finally {
			if (latch != null) {
				latch.countDown();
			}
		}
	}

}
