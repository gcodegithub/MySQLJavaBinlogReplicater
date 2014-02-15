package cn.ce.web.rest.impl;

import cn.ce.binlog.vo.OpParseResultVO;
import cn.ce.binlog.vo.TokenAuthRes;
import cn.ce.web.rest.i.IFOplogService;

public class OplogServiceImpl implements IFOplogService {

	public TokenAuthRes getToken(String slaveId, String tokenInput) {
		TokenAuthRes res = new TokenAuthRes();
//		try {
//			BinlogParserManager.getToken(new Long(slaveId), res);
//		} catch (Throwable ex) {
//			res = new TokenAuthRes();
//			res.setMsgDetail("Error,msg:" + ex.getMessage() + ", slaveId:"
//					+ slaveId);
//			res.setResCode(Const.ERROR);
//			ex.printStackTrace();
//		}
		return res;
	}

	public OpParseResultVO getDump(String slaveId, String binlogfilename,
			String serverhost, String serverPort, String username,
			String password, String tokenInput) {
		OpParseResultVO resVO = new OpParseResultVO();
//		try {
//			Mongo mongo = new Mongo(serverhost, new Integer(serverPort));
//
//			DB db = mongo.getDB("local");
//			DBCollection collection = db.getCollection("oplog.$main");
//			BasicDBObject search = new BasicDBObject();
//			search.put("op", new BasicDBObject("$ne", "n"));
//
//			DBCursor cursor = collection.find(search);
//			while (cursor.hasNext()) {
//				OpEventVO eventVO = new OpEventVO();
//				eventVO.setOneRec(cursor.next().toString());
//				resVO.addEventVOList(eventVO);
//			}
//
//		} catch (Throwable ex) {
//			StringBuilder sb = new StringBuilder();
//			sb.append("Error,msg:");
//			sb.append(ex.getMessage());
//			sb.append(",slaveId:");
//			sb.append(slaveId);
//			// sb.append(",binlogfilename:");
//			// sb.append(binlogfilename);
//			// sb.append(",binlogPosition:");
//			// sb.append(binlogPosition);
//			// sb.append(",serverhost:");
//			// sb.append(serverhost);
//			// sb.append(",serverPort:");
//			// sb.append(serverPort);
//			// sb.append(",username:");
//			// sb.append(username);
//			// sb.append(",tokenInput:");
//			// sb.append(tokenInput);
//			resVO.addErrorMsg(sb.toString());
//			resVO.setResCode(Const.ERROR);
//			ex.printStackTrace();
//		}
		return resVO;
	}

}
