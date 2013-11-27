package cn.ce.binlog.session;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.conv.MySQLEventConsumer;
import cn.ce.binlog.mysql.parse.BinlogParser;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.common.StringUtil;
import cn.ce.utils.mail.Alarm;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.EventVO;
import cn.ce.web.rest.vo.TokenAuthRes;

public class BinlogParserManager {
	private final static Log logger = LogFactory
			.getLog(BinlogParserManager.class);
	private final static BinlogParser dao = new BinlogParser();
	private final static MySQLEventConsumer consumerDao = new MySQLEventConsumer();
	private static ExecutorService executor = Executors.newFixedThreadPool(10);

	public static final ConcurrentHashMap<String, BinlogParseSession> sessionMap = new ConcurrentHashMap<String, BinlogParseSession>();

	public static void save2file(MysqlConnector c, String slaveId,
			BinlogParseSession bps, BinParseResultVO resVo) throws Throwable {
		BuzzWorker worker = new BuzzWorker(bps, resVo, consumerDao, "save2File");
		BinlogParserManager.doBuzzToExePool(worker);

	}

	public static void getToken(Long slaveId, TokenAuthRes res)
			throws Exception {
		String tokenFileAbspath = ProFileUtil
				.findMsgString(Const.sysconfigFileClasspath,
						"binlogparse.token.fullpath.file");
		String key = slaveId.toString();
		String tokenInFile = ProFileUtil.getValueFromProAbsPath(
				tokenFileAbspath, key);
		if (StringUtil.isBlank(tokenInFile)) {
			// 文件中不存在slaveId的token
			tokenInFile = String.valueOf(System.currentTimeMillis());
			ProFileUtil.modifyOrCreatePropertiesWithFileLock(tokenFileAbspath,
					key, tokenInFile, false, false);
			res.setMsgDetail("first visit,generate new token.");
			res.setResCode(TokenAuthRes.NEW_TOKEN);
			res.setNewToken(tokenInFile);
		} else {
			throw new Exception(
					"This SlaveId has already owned a Token,pls show your Token，SlaveId："
							+ slaveId);
		}
		// 驗證通過
		res.setMsgDetail("auth token generate");
		res.setResCode(TokenAuthRes.OK);
	}

	public static void auth(Long slaveId, String tokenInput) throws Exception {
		String tokenFileAbspath = ProFileUtil
				.findMsgString(Const.sysconfigFileClasspath,
						"binlogparse.token.fullpath.file");
		String key = slaveId.toString();
		String tokenInFile = ProFileUtil.getValueFromProAbsPath(
				tokenFileAbspath, key);
		if (StringUtil.isBlank(tokenInFile)) {
			// 文件中不存在slaveId的token
			throw new Exception("未注册slaveId,请先调用getToken进行注册");
		}
		if (!tokenInFile.equals(tokenInput)) {
			// token不合法
			throw new Exception("token is not valid for slaveId:" + slaveId);
		}
	}

	public static void startDumpToSession(Long slaveId, String binlogfilename,
			String binlogPosition, MysqlConnector c,
			BinlogParseSession parseSession, BinParseResultVO resVo)
			throws Throwable {
		if (BinlogParserManager.sessionMap.containsKey(slaveId.toString())) {
			// // one slaveId,one thread
			throw new Exception(
					"Error：one slaveId,one request,已经有线程正在处理该slaveId请求,slaveId="
							+ slaveId);
		}
		logger.info("处理开始");
		String filenameKey = slaveId + ".filenameKey";
		String binlogPositionKey = slaveId + ".binlogPosition";
		BinlogParserManager.sessionMap.put(slaveId.toString(), parseSession);
		//
		String posFileAbspath = ProFileUtil.findMsgString(
				Const.sysconfigFileClasspath,
				"binlogparse.checkpoint.fullpath.file");
		// 不输入检查点信息则从配置文件读取
		if (StringUtils.isBlank(binlogfilename) || binlogPosition == null
				|| new Long(binlogPosition) < 4) {
			binlogfilename = ProFileUtil.getValueFromProAbsPath(posFileAbspath,
					filenameKey);
			binlogPosition = ProFileUtil.getValueFromProAbsPath(posFileAbspath,
					binlogPositionKey);
			if (binlogfilename == null || binlogPosition == null) {
				// throw new Exception("Error:无法取得日志检查点信息");
				System.out.println("没有输入检查点信息，从头开始");
				// 默认从头开始
				binlogfilename = "mysql-bin.000001";
				binlogPosition = "4";
			}
		}
		resVo.setBinlogfilenameNext(binlogfilename);
		resVo.setBinlogPositionNext(new Long(binlogPosition));
		c.connect();
		parseSession.setC(c);
		parseSession.setLogPosition(binlogfilename, new Long(binlogPosition));
		parseSession.setSlaveId(slaveId);
		//
		BuzzWorker worker = new BuzzWorker(parseSession, resVo, dao,
				"startDump");
		BinlogParserManager.doBuzzToExePool(worker);

	}

	public static void getVOFromSession(BinParseResultVO resVo, Long slaveId,
			boolean isNeedWait) throws Exception {
		BinlogParseSession parseSession = BinlogParserManager.sessionMap
				.get(slaveId.toString());
		consumerDao.event2vo(parseSession, resVo, isNeedWait);
		if (StringUtils.isBlank(resVo.getBinlogfilenameNext())
				|| resVo.getBinlogPositionNext() == null) {
			throw new Exception("Error:无法定位检查点位置，无法保存检查点状态");

		}

	}

	public static void saveCheckPoint(BinParseResultVO resVo, long slaveId)
			throws Exception {
		String posFileAbspath = ProFileUtil.findMsgString(
				Const.sysconfigFileClasspath,
				"binlogparse.checkpoint.fullpath.file");
		String filenameKey = slaveId + ".filenameKey";
		String binlogPositionKey = slaveId + ".binlogPosition";
		String binlogfileName = resVo.getBinlogfilenameNext();
		String pos = resVo.getBinlogPositionNext().toString();
		ProFileUtil.modifyPropertieWithOutFileLock(posFileAbspath, filenameKey,
				binlogfileName, false, false);
		ProFileUtil.modifyOrCreatePropertiesWithFileLock(posFileAbspath,
				binlogPositionKey, pos, false, false);
	}

	private static void doBuzzToExePool(
			Callable<? extends BinParseResultVO> callable) throws Throwable {
		@SuppressWarnings("rawtypes")
		FutureTask<? extends BinParseResultVO> task = new FutureTask(callable);
		executor.submit(task);

	}

	public static void main(String[] args) {
		BinParseResultVO resVO = new BinParseResultVO();
		BinlogParseSession bps = new BinlogParseSession();
		TokenAuthRes res = new TokenAuthRes();
		try {
			long slaveId = 1113L;
			BinlogParserManager.getToken(slaveId, res);
			logger.info("auth res:" + res);

			String binlogfilename = "mysql-bin.000001";
			String binlogPosition = "107";
			// String binlogfilename = null;
			// Long binlogPosition = null;
			String serverhost = "192.168.215.1";
			int serverPort = 3306;
			String username = "canal";
			String password = "canal";
			// "192.168.215.1", 3306, "canal", "canal"
			MysqlConnector c = new MysqlConnector(serverhost, new Integer(
					serverPort), username, password);
			BinlogParserManager.startDumpToSession(slaveId, binlogfilename,
					binlogPosition, c, bps, resVO);
			logger.info("parseRes vo:" + resVO);
			System.out.println("----------OVER------------");
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
