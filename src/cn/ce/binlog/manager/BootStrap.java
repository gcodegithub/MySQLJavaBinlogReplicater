package cn.ce.binlog.manager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import cn.ce.binlog.zk.Master;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;

public class BootStrap implements InitializingBean, DisposableBean {

	private final static Log logger = LogFactory.getLog(BootStrap.class);

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
		m.init();
		Context context = m.getContext();
		String zkConInfo = context.getZkConInfo();
		if (StringUtils.isBlank(zkConInfo)) {
			m.begin();
		} else {
			Master master = new Master(context);
		}

		logger.info("-----------Spring容器完全启动---------------");
	}

	public void destroy() throws Exception {
		logger.info("-----------Spring容器销毁---------------");
		Context context = m.getContext();
		String zkConInfo = context.getZkConInfo();
		m.stop();
		if (!StringUtils.isBlank(zkConInfo)) {
			Master master = context.getM();
			master.close();
		}

		logger.info("-----------Spring容器完全退出--------------");
	}

	public static void main(String[] args) {
		try {
			BootStrap bs = new BootStrap();
			bs.afterPropertiesSet();
			logger.info("-----------OVER---------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
