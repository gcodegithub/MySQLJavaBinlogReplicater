package cn.ce.binlog.zk;

import java.util.List;

public interface RecoveryCallback {
	final static int OK = 0;
	final static int FAILED = -1;
	public void recoveryComplete(int rc, List<String> tasks);
}
