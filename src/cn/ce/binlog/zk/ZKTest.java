package cn.ce.binlog.zk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ce.binlog.manager.Context;

public class ZKTest {
	private static final Logger logger = LoggerFactory.getLogger(ZKTest.class);

	public static void takeMaster() {
		String zkConInfo = "127.0.0.1:2181";
		Long slaveId = new Long(System.currentTimeMillis());
		String zkClusterId = "1";
		Context context = new Context();
		context.setZkConInfo(zkConInfo);
		context.setSlaveId(slaveId);
		context.setZkClusterId(zkClusterId);
		Master m = new Master(context);
		while (!m.isExpired()) {
			try {
				logger.info("退回主thread:" + Thread.currentThread());
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		m.close();
	}

	public static void main(String[] args) {
		System.out.println("主线程，thread:" + Thread.currentThread());
		ZKTest.takeMaster();
		System.out.println("---------ZKTest OVER-------------");
	}

}
