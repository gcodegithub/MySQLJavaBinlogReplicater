package cn.ce.binlog.manager;

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.conv.ConsumeDaoIF;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.binlog.mysql.util.ThreadPoolUtils;
import cn.ce.binlog.session.BuzzWorker;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;

public class MySQLSourceManager extends AbsManager {
	private final static Log logger = LogFactory
			.getLog(MySQLSourceManager.class);
	private final BinlogDumper producter = new BinlogDumper();
	private final BinlogEventConsumer consumer = new BinlogEventConsumer();

	private final ProcessInfo process = new ProcessInfo();

	@Override
	public void begin() {
		try {
			this.stop();
			this.init();
			System.out.println("-----------BINLOG工程容器准备启动---------------");
			BuzzWorker<BinlogDumper, Context, ProcessInfo> p = new BuzzWorker<BinlogDumper, Context, ProcessInfo>(
					producter, context, process, "dump", null);
			ThreadPoolUtils.doBuzzToExePool(p);
			BuzzWorker<BinlogEventConsumer, Context, ProcessInfo> c = new BuzzWorker<BinlogEventConsumer, Context, ProcessInfo>(
					consumer, context, process, "consume", null);
			ThreadPoolUtils.doBuzzToExePool(c);
			System.out.println("-----------BINLOG工程容器启动完成---------------");
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
			//
			String posFileAbspath = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"binlogparse.checkpoint.fullpath.file");
			logger.info("检查点文件为："+posFileAbspath);
			context.setBinlogcheckfile(posFileAbspath);
			//
			String serverhost = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.mysql.master.ip");
			String serverPort = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.mysql.master.port");
			String username = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.mysql.master.user");
			String password = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.mysql.master.pass");
			MysqlConnector c = new MysqlConnector(serverhost, new Integer(
					serverPort), username, password);
			String slaveId = context.getSlaveId().toString();
			logger.info("slaveId为："+slaveId);
			String binlogfilename = ProFileUtil.getValueFromProAbsPath(
					posFileAbspath, slaveId + ".filenameKey");
			String binlogPosition = ProFileUtil.getValueFromProAbsPath(
					posFileAbspath, slaveId + ".binlogPosition");
			if (binlogfilename == null || binlogPosition == null) {
				logger.info("没有输入检查点信息，从默认开始");
				binlogfilename = "mysql-bin.000001";
				binlogPosition = "4";
			}
			context.setServerhost(serverhost);
			context.setServerPort(new Integer(serverPort));
			context.setUsername(username);
			context.setPassword(password);
			context.setC(c);
			context.setBinlogfilename(binlogfilename);
			context.setBinlogPosition(new Long(binlogPosition));
			c.reconnect();
			MysqlConnector nc = c.clone();
			nc.reconnect();
			TableMetaCache tableMetaCache = new TableMetaCache(nc,
					context.getSlaveId());
			context.setTableMetaCache(tableMetaCache);
			//
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	public static void main(String[] args) {
		AbsManager m = new MySQLSourceManager();
		try {
			m.init();
			m.begin();
			m.begin();
		} catch (Throwable e) {
			e.printStackTrace();

		}
		System.out.println("----------OVER---------");
	}

}
