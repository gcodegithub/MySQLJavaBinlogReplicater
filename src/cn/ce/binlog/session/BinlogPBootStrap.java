package cn.ce.binlog.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import cn.ce.binlog.manager.AbsManager;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;

public class BinlogPBootStrap implements InitializingBean, DisposableBean {

	private final static Log logger = LogFactory.getLog(BinlogPBootStrap.class);

	private final static AbsManager m;

	static {
		try {
			String clazz = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "manager.impclass");
			m = (AbsManager) Class.forName(clazz).newInstance();
		} catch (Exception e) {
			String msg = e.getMessage();
			e.printStackTrace();
			throw new RuntimeException(msg);
		}
	}

	public void afterPropertiesSet() {
		logger.info("-----------Spring容器销毁---------------");
		m.begin();
		logger.info("-----------Spring容器完全启动---------------");
	}

	public void destroy() throws Exception {
		logger.info("-----------Spring容器销毁---------------");
		m.stop();
		logger.info("-----------Spring容器完全退出--------------");
	}

	public static void main(String[] args) {
		try {
			BinlogPBootStrap bs = new BinlogPBootStrap();
			bs.afterPropertiesSet();
			logger.info("-----------OVER---------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
