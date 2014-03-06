package cn.ce.binlog.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ce.binlog.manager.Context;

public class MasterExistsWatcher implements Watcher {
	private static final Logger logger = LoggerFactory
			.getLogger(MasterExistsWatcher.class);
	private Context context;
	private Master master;

	public MasterExistsWatcher(Master master, Context context) {
		this.context = context;
		this.master = master;
	}

	public void process(WatchedEvent e) {
		logger.info("MasterExistsWatcher event:" + e);
		if (e.getType() == EventType.NodeDeleted) {
			String masterPathName = master.getMasterPath();
			assert masterPathName.equals(e.getPath());
			master.createMasterInfo();
			;
		}
	}
}
