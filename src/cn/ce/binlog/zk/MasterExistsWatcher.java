package cn.ce.binlog.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import cn.ce.binlog.manager.Context;

public class MasterExistsWatcher implements Watcher {
	private Context context;
	private Master master;
	private String masterPathName=master.getMasterPath();

	public MasterExistsWatcher(Master master, Context context) {
		this.context = context;
		this.master=master;
	}

	public void process(WatchedEvent e) {
		if (e.getType() == EventType.NodeDeleted) {
			assert masterPathName.equals(e.getPath());
			master.runForMaster();
		}
	}
}
