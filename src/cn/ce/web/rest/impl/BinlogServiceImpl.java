package cn.ce.web.rest.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.manager.BootStrap;
import cn.ce.binlog.vo.ControlBinlogXmlVO;
import cn.ce.binlog.vo.TokenAuthRes;
import cn.ce.cons.Const;
import cn.ce.utils.SpringUtil;
import cn.ce.web.rest.i.IFBinlogService;

public class BinlogServiceImpl implements IFBinlogService {
	private final static Log logger = LogFactory
			.getLog(BinlogServiceImpl.class);

	public TokenAuthRes getToken(String slaveId, String tokenInput) {
		TokenAuthRes res = new TokenAuthRes();
		// try {
		// BinlogParserManager.getToken(new Long(slaveId), res);
		// } catch (Throwable ex) {
		// res = new TokenAuthRes();
		// res.setMsgDetail("Error,msg:" + ex.getMessage() + ", slaveId:"
		// + slaveId);
		// res.setResCode(Const.ERROR);
		// ex.printStackTrace();
		// }
		return res;
	}

	public ControlBinlogXmlVO startBinlogXML() {
		ControlBinlogXmlVO res = new ControlBinlogXmlVO();
		try {
			BootStrap bootStrap = (BootStrap) SpringUtil.getBean("bootStrap");
			bootStrap.afterPropertiesSet();
			res.setMsgDetail("Binlog 工程成功启动");
			res.setResCode(Const.OK);
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			res.setMsgDetail(err);
			res.setResCode(Const.ERROR);
		}
		return res;
	}

	public ControlBinlogXmlVO stopBinlogXML() throws InterruptedException {
		ControlBinlogXmlVO res = new ControlBinlogXmlVO();
		try {
			BootStrap bootStrap = (BootStrap) SpringUtil.getBean("bootStrap");
			bootStrap.destroy();
			res.setMsgDetail("Binlog 工程完全停止");
			res.setResCode(Const.OK);
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			res.setMsgDetail(err);
			res.setResCode(Const.ERROR);
		}
		return res;
	}
}
