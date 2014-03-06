package cn.ce.binlog.zk;

import java.io.Closeable;
import java.io.IOException;

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

import cn.ce.binlog.manager.AbsManager;
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
	private String masterPath;
	private Watcher checkMasterExistsWatcher;

	private StringCallback masterCreateCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx, String name) {
			Context context = (Context) ctx;
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				logger.info("masterCreateCallback 连接丢失，继续获取主节点信息");
				getMasterInfo();
				break;
			case OK:
				state = MasterStates.ELECTED;
				logger.info("masterCreateCallback 成功上位");
				takeLeadership(context);
				break;
			case NODEEXISTS:
				state = MasterStates.NOTELECTED;
				logger.info("masterCreateCallback 节点已存在，继续检查主节点");
				checkMasterExists();
				break;
			default:
				state = MasterStates.NOTELECTED;
				logger.error("Something went wrong when running for master.",
						KeeperException.create(Code.get(rc), path));
			}
			String projectId = context.getProjectId();
			logger.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ")
					+ "the leader,projectId:" + projectId);
		}
	};

	private DataCallback getMasterInfoCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			Context context = (Context) ctx;
			String projectId = context.getProjectId();
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				logger.info("getMasterInfoCallback 连接丢失，继续获取主节点信息");
				getMasterInfo();
				break;
			case NONODE:
				logger.info("getMasterInfoCallback 节点不存在，准备竞选");
				createMasterInfo();
				break;
			case OK:
				if (projectId.equals(new String(data))) {
					state = MasterStates.ELECTED;
					logger.info("getMasterInfoCallback 节点存在，slaveId就是自己,准备上位");
					takeLeadership(context);
				} else {
					state = MasterStates.NOTELECTED;
					logger.info("getMasterInfoCallback 节点存在，slaveId不是自己,继续检查主节点");
					checkMasterExists();
				}
				break;
			default:
				logger.error("Error when reading data.",
						KeeperException.create(Code.get(rc), path));
			}
		}
	};

	private StatCallback checkMasterExistsCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
			case CONNECTIONLOSS:
				logger.info("checkMasterExistsCallback 连接丢失,继续检查主节点");
				checkMasterExists();
				break;
			case OK:
				if (stat == null) {
					logger.info("checkMasterExistsCallback 没有状态信息，准备竞选");
					state = MasterStates.RUNNING;
					createMasterInfo();
					logger.info("It sounds like the previous master is gone, "
							+ "so let's run for master again.");
				}
				break;
			default:
				logger.info("checkMasterExistsCallback ，准备获取主节点信息");
				getMasterInfo();
				break;
			}
		}
	};

	public MasterStates getState() {
		return state;
	}

	public Master(Context context) {
		this.context = context;
		this.startZK();
		checkMasterExistsWatcher = new MasterExistsWatcher(this, context);
		this.createMasterInfo();
		context.setM(this);
	}

	public String getMasterPath() {
		if (!StringUtils.isBlank(masterPath)) {
			return masterPath;
		}
		String zkClusterId = context.getZkClusterId().trim();
		masterPath = "/" + zkClusterId + "_" + "master";
		return masterPath;
	}

	@Override
	public void close() {
		try {
			this.stopZK();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("关闭zk连接失败，详情参阅以上异常栈信息.");
		}

	}

	@Override
	public void process(WatchedEvent e) {
		logger.info("called by the init zk,MasterWatcher event:" + e);
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

	public void createMasterInfo() {
		try {
			this.startZK();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		String masterPath = this.getMasterPath();
		logger.info("Running for master,masterPath in zk:" + masterPath);
		String projectId = context.getProjectId();
		logger.info("确保不同工程拥有不同projectId，projectId:" + projectId);
		zk.create(masterPath, projectId.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL, masterCreateCallback, context);
	}

	public boolean isExpired() {
		return expired;
	}

	@Override
	protected void finalize() {
		this.close();
	}

	// ------------private
	private void startZK() {

		if (zk != null) {
			return;
		}
		try {
			String zkConInfo = context.getZkConInfo();
			if (StringUtils.isBlank(zkConInfo)) {
				throw new RuntimeException(
						"zookeeper connection info is null,zkConInfo:"
								+ zkConInfo);
			}
			zk = new ZooKeeper(zkConInfo, 5000, this);
		} catch (IOException e) {
			String err = e.getMessage();
			e.printStackTrace();
			throw new RuntimeException(err);
		}
	}

	private synchronized void stopZK() throws InterruptedException, IOException {
		if (zk != null) {
			zk.close();
			zk = null;
		}
	}

	private void getMasterInfo() {
		String masterPath = this.getMasterPath();
		zk.getData(masterPath, false, getMasterInfoCallback, context);
	}

	private void checkMasterExists() {
		String masterPath = this.getMasterPath();
		zk.exists(masterPath, checkMasterExistsWatcher,
				checkMasterExistsCallback, context);
	}

	private void takeLeadership(Context context) {
		logger.info("成为主节点，进行业务处理");
		// while (true) {
		// try {
		// Thread.sleep(100000);
		// logger.info("成为主节点，进行业务处理,thread:" + Thread.currentThread());
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
		AbsManager manager = context.getManager();
		manager.begin();
		// getWorkers();
		// (new RecoveredAssignments(zk)).recover(new RecoveryCallback() {
		// public void recoveryComplete(int rc, List tasks) {
		// if (rc == RecoveryCallback.FAILED) {
		// logger.error("Recovery of assigned tasks failed.");
		// } else {
		// logger.info("Assigning recovered tasks");
		// // getTasks();
		// }
		// }
		// });
	}

}
