package cn.ce.binlog.session;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.web.rest.vo.BinParseResultVO;

//执行线程
public class BuzzWorker<R extends BinParseResultVO, S extends BinlogParseSession, D>
		implements Callable<R> {

	private final static Log logger = LogFactory.getLog(BuzzWorker.class);

	private final D dao;
	private final S session;
	private final R res;
	private final String invokeMethodName;

	public BuzzWorker(S session, R res, D dao, String invokeMethodName) {
		if (dao == null || session == null || res == null
				|| StringUtils.isBlank(invokeMethodName)) {
			throw new RuntimeException("线程执行單元初始化失败！");
		}
		this.dao = dao;
		this.session = session;
		this.res = res;
		this.invokeMethodName = invokeMethodName;
	}

	public R call() throws Exception {
		// 校验信息
		Method m;
		try {
			m = dao.getClass().getMethod(invokeMethodName, session.getClass(),
					res.getClass());
			m.invoke(dao, session, res);
		} catch (NoSuchMethodException e) {
			res.addErrorMsg(e.getMessage());
			logger.fatal("线程调用方法处理业务失败，invokeMethodName=" + invokeMethodName);
			e.printStackTrace();
			throw e;
		}
		System.out.print(invokeMethodName);
		return res;
	}
}
