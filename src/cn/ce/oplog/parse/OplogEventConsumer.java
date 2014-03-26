package cn.ce.oplog.parse;

import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.types.BSONTimestamp;
import org.bson.types.ObjectId;

import cn.ce.binlog.manager.AbsDataConsumer;
import cn.ce.binlog.manager.Context;
import cn.ce.binlog.manager.ProcessInfo;
import cn.ce.binlog.mongo.simple.MongoConnectionFactory;
import cn.ce.binlog.vo.OpParseResultVO;
import cn.ce.binlog.vo.TransCommandVO;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.mail.Alarm;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

public class OplogEventConsumer extends AbsDataConsumer {

	private final static Log logger = LogFactory
			.getLog(OplogEventConsumer.class);

	public void consume(final Context context, final ProcessInfo processInfo,
			Object[] params) {
		try {
			while (!context.isPrepareStop()) {
				try {
					this.consumeFrame(context, processInfo, params);
				} catch (MongoException.Network ex) {
					String msg = ex.getMessage();
					ex.printStackTrace();
					System.err.println("目标Mongodb连接断开，准备重新连接，err:" + msg);
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
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			err = "xml binlog文件持久化线程停止，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n"
					+ context.toString() + "\n");
		} finally {
			context.setPrepareStop(true);
			context.setConsumerThreadStop(true);
			logger.info("---------MySQLEventConsumer持久化文件线程结束!!----------------");
		}
	}

	@Override
	protected TransCommandVO getTransCommandVO(Object rv, Context context)
			throws Exception {
		OpParseResultVO resVo = (OpParseResultVO) rv;
		TransCommandVO tcvo = new TransCommandVO(resVo, context);
		return tcvo;
	}

	@Override
	protected Object event2vo(Context context) throws Exception {
		int len = context.getDbObjectQueueSize();
		OpParseResultVO resVo = new OpParseResultVO();
		for (int i = 1; i <= len; i++) {
			DBObject dbObject = context.getDbObjectQueue();
			String ns = dbObject.get("ns").toString();
			if (StringUtils.isBlank(ns)) {
				continue;
			}
			BSONTimestamp tsObj = (BSONTimestamp) dbObject.get("ts");
			Integer ts = tsObj.getTime();
			long when = new Long(ts);
			Integer inc = tsObj.getInc();
			if (ts == null || inc == null) {
				continue;
			}
//			context.setOplogtsInt(ts);
//			context.setOplogincInt(inc);
			resVo.setTimestamp(ts);
			resVo.setInc(inc.toString());
			// oplog更新时分为全部更新和局部更新，数据格式不同。
			/*
			 * { "ts" : { "$ts" : 1392344621 , "$inc" : 1} , "op" : "u" , "ns" :
			 * "hawaii.userinfo" , "o2" : { "_id" : { "$oid" :
			 * "52fd761fd9e7bd06215bb7cf"}} , "o" : { "$set" : { "text" :
			 * "name_2"}}}
			 */
			DBObject value = (DBObject) dbObject.get("o");
			value.put("when", when);
			value.put("ns", ns);
			value.put("op", dbObject.get("op"));
			String prikey = "_id";
			if (dbObject.get("op").equals("u")) {
				if (value.get("$set") != null) {
					// 获取本地更新的文档
					Object priValue = ((DBObject) dbObject.get("o2"))
							.get(prikey);
					DBObject newValue = new BasicDBObject();
					newValue.putAll(value);
					newValue.put(prikey, priValue);
					value = newValue;
				}

			}

			Object priValue = value.get(prikey);
			if (priValue != null) {
				resVo.addEventVOList(value);
			}

		}// for end
		return resVo;
	}

	@Override
	protected void saveCheckPoint(Object rv, Context context) throws Exception {
		OpParseResultVO resVo = (OpParseResultVO) rv;
		String posFileAbspath = context.getOplogcheckfile();
		String tskey = "mongodb.checkpoint.ts";
		String inckey = "mongodb.checkpoint.inc";
		String ts = resVo.getTimestamp().toString();
		String inc = resVo.getInc();
		Map<String, String> keyvalue = new HashMap<String, String>();
		keyvalue.put(tskey, ts);
		keyvalue.put(inckey, inc);
		if (!StringUtils.isBlank(ts)) {
			ProFileUtil.modifyPropertieWithOutFileLock(posFileAbspath,
					keyvalue, false, false);
		}

	}

	@Override
	protected int getQueueSize(Context context) throws Exception {
		return context.getDbObjectQueueSize();
	}
}
