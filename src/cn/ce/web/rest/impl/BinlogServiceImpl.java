package cn.ce.web.rest.impl;

import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.session.BinlogPBootStrap;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.BinlogParserManager;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.mail.Alarm;
import cn.ce.web.rest.i.IFBinlogService;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.ControlBinlogXmlVO;
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
			res.setResCode(Const.ERROR);
			ex.printStackTrace();
		}
		return res;
	}

	public BinParseResultVO getDump(Long slaveId, String binlogfilename,
			String binlogPosition, String serverhost, Integer serverPort,
			String username, String password, String tokenInput) {
		BinParseResultVO resVO = new BinParseResultVO();
		BinlogParseSession bps = new BinlogParseSession();
		MysqlConnector c = new MysqlConnector(serverhost, serverPort, username,
				password);
		try {
			BinlogParserManager.auth(slaveId, tokenInput);
			BinlogParserManager.startDumpToSession(slaveId, binlogfilename,
					binlogPosition, c, bps, resVO);
			boolean isNeedWait = true;
			BinlogParserManager.getVOFromSession(resVO, slaveId, isNeedWait);
			BinlogParserManager.saveCheckPoint(resVO, slaveId);
		} catch (Throwable ex) {
			StringBuilder sb = new StringBuilder();
			sb.append("Error,msg:");
			sb.append(ex.getMessage());
			sb.append(",slaveId:");
			sb.append(slaveId);
			resVO.addErrorMsg(sb.toString());
			resVO.setResCode(Const.ERROR);
			ex.printStackTrace();
		} finally {
			System.out.println("-----------webservice 调用完成---------------");
			c.disconnect();
			BinlogParserManager.sessionMap.remove(slaveId.toString());
		}
		return resVO;
	}

	public ControlBinlogXmlVO startBinlogXML() {
		BinlogPBootStrap bpbs = new BinlogPBootStrap();
		ControlBinlogXmlVO res = new ControlBinlogXmlVO();
		try {
			String slaveId = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"bootstrap.mysql.master.slaveid");
			BinParseResultVO resVo = new BinParseResultVO();
			bpbs.initTarget();
			bpbs.contiParsebinlog(resVo, slaveId);
			BinlogParserManager.consumer(bpbs.getBps().getC(), slaveId,
					bpbs.getBps(), resVo);
			res.setMsgDetail("Binlog XML工程成功启动");
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
		BinlogParseSession bps = null;
		try {
			String slaveId = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath,
					"bootstrap.mysql.master.slaveid");
			bps = BinlogParserManager.sessionMap.get(slaveId);
			if (bps == null) {
				System.out.println("-----------给出多slaveId不在本程序中存在，slaveId："
						+ slaveId);
				res.setMsgDetail("给出的slaveId不在本程序中存在，slaveId：" + slaveId);
				res.setResCode(Const.ERROR);
				return res;
			}
			bps.getC().setPrepareStop(true);
			while (!(bps.getConsumerThreadStop() && bps.getParseThreadStop())) {
				Thread.sleep(500);
				if (!bps.getConsumerThreadStop()) {
					bps.getConsumerThread().interrupt();
				}
				if (!bps.getParseThreadStop()) {
					bps.getParseThread().interrupt();
				}
			}
			res.setMsgDetail("Binlog XML工程完全退出");
			res.setResCode(Const.OK);
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			res.setMsgDetail(err);
			res.setResCode(Const.ERROR);
		} finally {
			System.out.println("-----------Binlog XML工程完全退出--------------");
			if (bps != null) {
				BinlogParserManager.sessionMap.remove(bps.getSlaveId()
						.toString());
			}

		}
		return res;
	}
}
