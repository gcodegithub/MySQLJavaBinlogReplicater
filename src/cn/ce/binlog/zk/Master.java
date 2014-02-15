package cn.ce.binlog.zk;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ce.binlog.manager.Context;

public class Master implements Watcher, Closeable {

	private static final Logger logger = LoggerFactory.getLogger(Master.class);

	/*
	 * RUNNING means that according to its view of the ZooKeeper state, there is
	 * no primary master (no master has been able to acquire the /master lock).
	 * If some master succeeds in creating the /master znode and this master
	 * learns it, then it transitions to ELECTED if it is the primary and
	 * NOTELECTED otherwise.
	 */
	enum MasterStates {
		RUNNING, ELECTED, NOTELECTED
	};

	private volatile MasterStates state = MasterStates.RUNNING;
	private Context context;
	private ZooKeeper zk;
	private volatile boolean connected = false;
	private volatile boolean expired = false;
	private String masterPath = this.getMasterPath();

	private Watcher masterExistsWatcher = new MasterExistsWatcher(this, context);

	private StringCallback masterCreateCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			Context context = (Context) ctx;
			String serverId = context.getSlaveId().toString();
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case OK:
				state = MasterStates.ELECTED;
				takeLeadership();
				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				masterExists();
				break;
			default:
				state = MasterStates.NOTELECTED;
				logger.error("Something went wrong when running for master.",
						KeeperException.create(Code.get(rc), path));
			}
			logger.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ")
					+ "the leader " + serverId);
		}
	};

	private DataCallback masterCheckCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			Context context = (Context) ctx;
			String serverId = context.getSlaveId().toString();
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				runForMaster();
				break;
			case OK:
				if (serverId.equals(new String(data))) {
					state = MasterStates.ELECTED;
					takeLeadership();
				} else {
					state = MasterStates.NOTELECTED;
					masterExists();
				}
				break;
			default:
				logger.error("Error when reading data.",
						KeeperException.create(Code.get(rc), path));
			}
		}
	};

	private StatCallback masterExistsCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();
				break;
			case OK:
				if (stat == null) {
					state = MasterStates.RUNNING;
					runForMaster();
					logger.info("It sounds like the previous master is gone, "
							+ "so let's run for master again.");
				}
				break;
			default:
				checkMaster();
				break;
			}
		}
	};

	MasterStates getState() {
		return state;
	}

	Master(Context context) {
		this.context = context;
	}

	public String getMasterPath() {
		if (!StringUtils.isBlank(masterPath)) {
			return masterPath;
		}
		String mysqlHostName = context.getC().getAddress().getHostName();
		masterPath = "/" + mysqlHostName + "_" + "master";
		return masterPath;
	}

	public void runForMaster() {
		try {
			this.startZK();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		String masterPath = this.getMasterPath();
		logger.info("Running for master,masterPath in zk:" + masterPath);
		String slaveId = context.getSlaveId().toString();
		zk.create(masterPath, slaveId.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL, masterCreateCallback, context);
	}

	@Override
	public void close() throws IOException {
		try {
			this.stopZK();
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void process(WatchedEvent e) {
		logger.info("Processing event: " + e.toString());
		if (e.getType() == Event.EventType.None) {
			switch (e.getState()) {
			case SyncConnected:
				this.connected = true;
				break;
			case Disconnected:
				this.connected = false;
				break;
			case Expired:
				expired = true;
				this.connected = false;
				logger.error("Session expiration");
			default:
				break;
			}
		}

	}

	// ------------private
	private void startZK() throws IOException, InterruptedException {
		String zkConInfo = context.getZkConInfo();
		if (StringUtils.isBlank(zkConInfo)) {
			throw new RuntimeException(
					"zookeeper connection info is null,zkConInfo:" + zkConInfo);
		}
		if (zk != null) {
			return;
		}
		zk = new ZooKeeper(zkConInfo, 5000, this);
	}

	private void stopZK() throws InterruptedException, IOException {
		if (zk != null) {
			zk.close();
		}

	}

	private void checkMaster() {
		String masterPath = this.getMasterPath();
		zk.getData(masterPath, false, masterCheckCallback, context);
	}

	private void masterExists() {
		String masterPath = this.getMasterPath();
		zk.exists(masterPath, masterExistsWatcher, masterExistsCallback,
				context);
	}

	private void takeLeadership() {
		logger.info("Going for list of workers");
		//getWorkers();
		(new RecoveredAssignments(zk)).recover(new RecoveryCallback() {
			public void recoveryComplete(int rc, List tasks) {
				if (rc == RecoveryCallback.FAILED) {
					logger.error("Recovery of assigned tasks failed.");
				} else {
					logger.info("Assigning recovered tasks");
					//getTasks();
				}
			}
		});
	}

}
