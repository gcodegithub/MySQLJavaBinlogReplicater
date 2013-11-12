package cn.ce.web.rest.impl;

import cn.ce.binlog.session.BinlogParserManager;
import cn.ce.web.rest.i.IFBinlogService;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.TokenAuthRes;

public class BinlogServiceImpl implements IFBinlogService {

	public TokenAuthRes getToken(String slaveId, String tokenInput) {
		TokenAuthRes res = new TokenAuthRes();
		try {
			BinlogParserManager.getToken(new Long(slaveId), res);
		} catch (Throwable ex) {
			res = new TokenAuthRes();
			res.setMsgDetail("Error,msg:" + ex.getMessage() + ", slaveId:"
					+ slaveId);
			res.setResCode(TokenAuthRes.ERROR);
			ex.printStackTrace();
		}
		return res;
	}

	public BinParseResultVO getDump(String slaveId, String binlogfilename,
			String binlogPosition, String serverhost, String serverPort,
			String username, String password, String tokenInput) {
		BinParseResultVO resVO = new BinParseResultVO();
		try {
			BinlogParserManager.auth(new Long(slaveId), tokenInput);
			BinlogParserManager.startDump(new Long(slaveId), binlogfilename,
					new Long(binlogPosition), serverhost, new Integer(
							serverPort), username, password, resVO);
		} catch (Throwable ex) {
			StringBuilder sb = new StringBuilder();
			sb.append("Error,msg:");
			sb.append(ex.getMessage());
			sb.append(",slaveId:");
			sb.append(slaveId);
//			sb.append(",binlogfilename:");
//			sb.append(binlogfilename);
//			sb.append(",binlogPosition:");
//			sb.append(binlogPosition);
//			sb.append(",serverhost:");
//			sb.append(serverhost);
//			sb.append(",serverPort:");
//			sb.append(serverPort);
//			sb.append(",username:");
//			sb.append(username);
//			sb.append(",tokenInput:");
//			sb.append(tokenInput);
			resVO.addErrorMsg(sb.toString());
			resVO.setResCode(TokenAuthRes.ERROR);
			ex.printStackTrace();
		}
		return resVO;
	}
}
