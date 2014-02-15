package cn.ce.binlog.session;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//执行线程---
public class BuzzWorker<D, S, R> implements Callable<R> {

	private final static Log logger = LogFactory.getLog(BuzzWorker.class);

	private final D dao;
	private final S session;
	private final R res;
	private final String invokeMethodName;
	private Object[] params;

	public BuzzWorker(D dao, S session, R res, String invokeMethodName,
			Object... params) {
		if (dao == null || session == null || res == null
				|| StringUtils.isBlank(invokeMethodName)) {
			throw new RuntimeException("线程执行單元初始化失败！");
		}
		this.dao = dao;
		this.session = session;
		this.res = res;
		this.invokeMethodName = invokeMethodName;
		this.params = params;
	}

	public R call() throws Exception {
		// 校验信息
		Method m;
		try {
			m = dao.getClass().getMethod(invokeMethodName, session.getClass(),
					res.getClass(),Object[].class);
			m.invoke(dao, session, res, params);
		} catch (Exception e) {
			logger.fatal("线程调用方法处理业务失败，invokeMethodName=" + invokeMethodName);
			e.printStackTrace();
			throw e;
		}
		return res;
	}
}
