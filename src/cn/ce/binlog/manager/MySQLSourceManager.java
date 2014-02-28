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
	private final static Log			logger		= LogFactory.getLog(MySQLSourceManager.class);
	private final BinlogDumper			producter	= new BinlogDumper();
	private final BinlogEventConsumer	consumer	= new BinlogEventConsumer();
	private Context						context;
	private final ProcessInfo			process		= new ProcessInfo();

	@Override
	public void begin() {
		try {
			this.stop();
			this.init();
			System.out.println("-----------BINLOG工程容器准备启动---------------");
			BuzzWorker<BinlogDumper, Context, ProcessInfo> p = new BuzzWorker<BinlogDumper, Context, ProcessInfo>(producter, context, process, "dump", null);
			ThreadPoolUtils.doBuzzToExePool(p);
			BuzzWorker<BinlogEventConsumer, Context, ProcessInfo> c = new BuzzWorker<BinlogEventConsumer, Context, ProcessInfo>(consumer, context, process, "consume", null);
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
			if (context.getParseThread() == null && context.getConsumerThread() == null) {
				return;
			}
			System.out.println("-----------工程容器准备关闭---------------");
			context.setPrepareStop(true);
			while (!(context.isParseThreadStop() && context.isConsumerThreadStop())) {
				Thread.sleep(500);
				if (!context.isParseThreadStop()) {
					context.getParseThread().interrupt();
				}
				if (!context.isConsumerThreadStop()) {
					context.getConsumerThread().interrupt();
				}
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
			context = new Context();
			context.setPrepareStop(false);
			String clazz = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "consu.impclass");

			Class[] parameter = new Class[] { Context.class };
			Constructor con = Class.forName(clazz).getConstructor(parameter);
			ConsumeDaoIF curd = (ConsumeDaoIF) con.newInstance(context);
			consumer.setCurd(curd);
			//
			String posFileAbspath = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "binlogparse.checkpoint.fullpath.file");
			context.setBinlogcheckfile(posFileAbspath);
			//
			String slaveId = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.slaveid");
			String serverhost = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mysql.master.ip");
			String serverPort = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mysql.master.port");
			String username = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mysql.master.user");
			String password = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mysql.master.pass");
			MysqlConnector c = new MysqlConnector(serverhost, new Integer(serverPort), username, password);
			String binlogfilename = ProFileUtil.getValueFromProAbsPath(posFileAbspath, slaveId + ".filenameKey");
			String binlogPosition = ProFileUtil.getValueFromProAbsPath(posFileAbspath, slaveId + ".binlogPosition");
			if (binlogfilename == null || binlogPosition == null) {
				logger.info("没有输入检查点信息，从默认开始");
				binlogfilename = "mysql-bin.000001";
				binlogPosition = "4";
			}
			context.setSlaveId(new Long(slaveId));
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
			TableMetaCache tableMetaCache = new TableMetaCache(nc, context.getSlaveId());
			context.setTableMetaCache(tableMetaCache);

			//
			String isMark_s = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "consu.ismark");
			Boolean isMarkDelete = new Boolean(isMark_s);
			context.setMarkDelete(isMarkDelete);
			//
			String desc_ipcsv = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.seeds");
			String desc_port_s = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.port");
			String desc_username = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.user");
			String desc_passwd = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.pass");
			String forcedbname = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.forcedbname");
			Integer desc_port = new Integer(desc_port_s);
			context.setDescMongoIpCSV(desc_ipcsv);
			context.setDescMongoPort(desc_port);
			context.setDescMongoUser(desc_username);
			context.setDescMongoPass(desc_passwd);
			context.setForcedbname(forcedbname);
			String connectionsPerHost_s = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.connectionsPerHost");
			String threadsAllowedToBlockForConnectionMultiplier_s = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mongo.threadsAllowedToBlockForConnectionMultiplier");
			context.setConnectionsPerHost_s(connectionsPerHost_s);
			context.setThreadsAllowedToBlockForConnectionMultiplier_s(threadsAllowedToBlockForConnectionMultiplier_s);

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
