package cn.ce.binlog.manager;

import java.lang.reflect.Constructor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.conv.ConsumeDaoIF;
import cn.ce.binlog.mysql.util.ThreadPoolUtils;
import cn.ce.binlog.session.BuzzWorker;
import cn.ce.cons.Const;
import cn.ce.oplog.parse.MongoDeltaInfo;
import cn.ce.oplog.parse.OplogEventConsumer;
import cn.ce.utils.common.ProFileUtil;

public class MongoDBSourceManager extends AbsManager {

	private final static Log logger = LogFactory
			.getLog(MongoDBSourceManager.class);

	private final static MongoDeltaInfo deltaInfo = new MongoDeltaInfo();
	private final static OplogEventConsumer consumer = new OplogEventConsumer();

	private final ProcessInfo process = new ProcessInfo();

	@Override
	public void init() {
		try {
			super.init();

			String clazz = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "consu.impclass");
			Class[] parameter = new Class[] { Context.class };
			Constructor con = Class.forName(clazz).getConstructor(parameter);
			ConsumeDaoIF curd = (ConsumeDaoIF) con.newInstance(context);
			consumer.setCurd(curd);
			String posFileAbspath = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"oplogparse.checkpoint.fullpath.file");
			context.setOplogcheckfile(posFileAbspath);

			//
			String oplogts = ProFileUtil.getValueFromProAbsPath(posFileAbspath,
					"mongodb.checkpoint.ts");
			String oploginc = ProFileUtil.getValueFromProAbsPath(
					posFileAbspath, "mongodb.checkpoint.inc");
			if (StringUtils.isBlank(oplogts)) {
				logger.info("没有输入检查点信息，从头开始");
				oplogts = "0";
				oploginc = "0";
			}
			context.setOplogtsInt(new Integer(oplogts));
			context.setOplogincInt(new Integer(oploginc));
			//
			String monitortb = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"bootstrap.source.mongo.monitortb");
			context.setMonitortb(monitortb);
			//

			String source_ipcsv = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"bootstrap.source.mongo.seeds");
			String source_port_s = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.source.mongo.port");
			String source_username = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.source.mongo.user");
			String source_passwd = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.source.mongo.pass");
			Integer source_port = new Integer(source_port_s);
			context.setSourceMongoIpCSV(source_ipcsv);
			context.setSourceMongoPort(source_port);
			context.setSourceMongoUser(source_username);
			context.setSourceMongoPass(source_passwd);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	public void begin() {
		try {
			this.stop();
			this.init();
			System.out.println("-----------OPLOG工程容器准备启动---------------");
			BuzzWorker<MongoDeltaInfo, Context, ProcessInfo> worker = new BuzzWorker<MongoDeltaInfo, Context, ProcessInfo>(
					deltaInfo, context, process, "deltaInfoGet");
			ThreadPoolUtils.doBuzzToExePool(worker);
			BuzzWorker<OplogEventConsumer, Context, ProcessInfo> c = new BuzzWorker<OplogEventConsumer, Context, ProcessInfo>(
					consumer, context, process, "consume", null);
			ThreadPoolUtils.doBuzzToExePool(c);
			System.out.println("-----------OPLOG工程容器启动完成---------------");
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void stop() {
		try {
			if (context == null) {
				return;
			}
			if (context.getParseThread() == null
					&& context.getConsumerThread() == null) {
				return;
			}
			System.out.println("-----------工程容器准备关闭---------------");
			context.setPrepareStop(true);
			while (!(context.isParseThreadStop() && context
					.isConsumerThreadStop())) {
				Thread.sleep(500);
				if (!context.isParseThreadStop()) {
					context.getParseThread().interrupt();
				}
//				if (!context.isConsumerThreadStop()) {
//					context.getConsumerThread().interrupt();
//				}
				System.out.println("-----------工程容器正在关闭---------------");
			}
			System.out.println("-----------工程容器完全关闭---------------");
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}
}
