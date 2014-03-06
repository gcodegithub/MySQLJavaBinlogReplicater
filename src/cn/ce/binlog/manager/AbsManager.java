package cn.ce.binlog.manager;

import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;

public abstract class AbsManager {
	protected Context context;

	public void init() {
		try {
			context = new Context();
			context.setPrepareStop(false);
			context.setManager(this);
			String projectId=ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "project.id");
			context.setProjectId(projectId);
			//
			String slaveId = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.slaveid");
			context.setSlaveId(new Long(slaveId));
			//
			String isMark_s = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "consu.ismark");
			Boolean isMarkDelete = new Boolean(isMark_s);
			context.setMarkDelete(isMarkDelete);
			//
			String desc_ipcsv = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.mongo.seeds");
			String desc_port_s = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.mongo.port");
			String desc_username = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.mongo.user");
			String desc_passwd = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.mongo.pass");
			String forcedbname = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.mongo.forcedbname");
			Integer desc_port = new Integer(desc_port_s);
			context.setDescMongoIpCSV(desc_ipcsv);
			context.setDescMongoPort(desc_port);
			context.setDescMongoUser(desc_username);
			context.setDescMongoPass(desc_passwd);
			context.setForcedbname(forcedbname);
			//
			String connectionsPerHost_s = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"bootstrap.mongo.connectionsPerHost");
			String threadsAllowedToBlockForConnectionMultiplier_s = ProFileUtil
					.findMsgString(Const.sysconfigFileClasspath,
							"bootstrap.mongo.threadsAllowedToBlockForConnectionMultiplier");
			context.setConnectionsPerHost_s(connectionsPerHost_s);
			context.setThreadsAllowedToBlockForConnectionMultiplier_s(threadsAllowedToBlockForConnectionMultiplier_s);
			//
			String zkConInfo_s = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.zk.coninfo");
			String zkClusterId_s = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "bootstrap.zk.clusterid");
			context.setZkConInfo(zkConInfo_s);
			context.setZkClusterId(zkClusterId_s);
		} catch (Throwable ex) {
			throw new RuntimeException(ex);
		}
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public abstract void begin();

	public abstract void stop();
}
