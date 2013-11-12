package cn.ce.binlog.session;

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
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.common.StringUtil;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.TokenAuthRes;

public class BinlogParserManager {
	private final static Log logger = LogFactory
			.getLog(BinlogParserManager.class);
	private final static BinlogParser dao = new BinlogParser();
	private static ExecutorService executor = Executors.newFixedThreadPool(10);

	public static final ConcurrentHashMap<Long, BinlogParseSession> sessionMap = new ConcurrentHashMap<Long, BinlogParseSession>();

	public static void getToken(Long slaveId, TokenAuthRes res)
			throws Exception {
		String tokenFileClasspath = "conf/clientIdToken.properties";
		String absPath = ProFileUtil.getFileAbsPath(tokenFileClasspath);
		String key = slaveId.toString();
		String tokenInFile = ProFileUtil.findMsgString(tokenFileClasspath, key);
		if (StringUtil.isBlank(tokenInFile)) {
			// 文件中不存在slaveId的token
			tokenInFile = String.valueOf(System.currentTimeMillis());
			ProFileUtil.modifyOrCreatePropertiesWithFileLock(absPath, key,
					tokenInFile, false, false);
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
		String tokenFileClasspath = "conf/clientIdToken.properties";
		String key = slaveId.toString();
		String tokenInFile = ProFileUtil.findMsgString(tokenFileClasspath, key);
		if (StringUtil.isBlank(tokenInFile)) {
			// 文件中不存在slaveId的token
			throw new Exception("未注册slaveId,请先调用getToken进行注册");
		}
		if (!tokenInFile.equals(tokenInput)) {
			// token不合法
			throw new Exception("token is not valid for slaveId:" + slaveId);
		}
	}

	public static void startDump(long slaveId, String binlogfilename,
			Long binlogPosition, String serverhost, int serverPort,
			String username, String password, BinParseResultVO resVo)
			throws Throwable {
		if (BinlogParserManager.sessionMap.containsKey(slaveId)) {
			// // one slaveId,one thread
			throw new Exception(
					"Error：one slaveId,one request,已经有线程正在处理该slaveId请求,slaveId="
							+ slaveId);
		}
		logger.info("处理开始");
		String filenameKey = slaveId + ".filenameKey";
		String binlogPositionKey = slaveId + ".binlogPosition";
		String posFileClasspath = "conf/binlogPosClintIdMap.properties";

		BinlogParseSession parseSession = new BinlogParseSession();
		BinlogParserManager.sessionMap.put(slaveId, parseSession);
		MysqlConnector c = new MysqlConnector(serverhost, serverPort, username,
				password);
		try {
			c.connect();
			parseSession.setC(c);
			parseSession.setLogPosition(binlogfilename, binlogPosition);
			parseSession.setSlaveId(slaveId);

			String absPath = ProFileUtil.getFileAbsPath(posFileClasspath);
			//
			// 不输入检查点信息则从配置文件读取
			if (StringUtils.isBlank(binlogfilename) || binlogPosition == null
					|| binlogPosition < 4) {
				binlogfilename = ProFileUtil.findMsgString(posFileClasspath,
						filenameKey);
				String binlogPositionString = ProFileUtil.findMsgString(
						posFileClasspath, binlogPositionKey);
				if (binlogfilename == null || binlogPositionString == null) {
					throw new Exception("Error:无法取得日志检查点信息");
				}
				binlogPosition = new Long(binlogPositionString);
			} else {
				resVo.setBinlogfilename(binlogfilename);
				resVo.setBinlogPosition(binlogPosition);
			}

			BuzzWorker worker = new BuzzWorker(parseSession, resVo, dao,
					"startDump");
			BinlogParserManager.doBuzzToExePool(worker);
			System.out.println("----------准备获取------------");
			MySQLEventConsumer.event2vo(parseSession, resVo);
			// logger.info("resVo:" + resVo);
			if (StringUtils.isBlank(resVo.getBinlogfilename())
					|| resVo.getBinlogPosition() == null) {
				throw new Exception("Error:无法定位检查点位置，无法保存检查点状态");

			}
			String binlogfileName = resVo.getBinlogfilename();
			String pos = resVo.getBinlogPosition().toString();
			ProFileUtil.modifyOrCreatePropertiesWithFileLock(absPath,
					filenameKey, binlogfileName, false, false);
			ProFileUtil.modifyOrCreatePropertiesWithFileLock(absPath,
					binlogPositionKey, pos, false, false);
		} finally {
			c.disconnect();
			sessionMap.remove(slaveId);
		}

	}

	private static void doBuzzToExePool(
			Callable<? extends BinParseResultVO> callable) throws Throwable {
		@SuppressWarnings("rawtypes")
		FutureTask<? extends BinParseResultVO> task = new FutureTask(callable);
		executor.submit(task);

	}

	public static void main(String[] args) {
		BinParseResultVO resVO = new BinParseResultVO();
		TokenAuthRes res = new TokenAuthRes();
		try {

			BinlogParserManager.getToken(1111L, res);
			logger.info("auth res:" + res);
			long slaveId = 111L;
			String binlogfilename = "mysql-bin.000001";
			Long binlogPosition = 107L;
			// String binlogfilename = null;
			// Long binlogPosition = null;
			String serverhost = "192.168.215.1";
			int serverPort = 3306;
			String username = "canal";
			String password = "canal";
			// "192.168.215.1", 3306, "canal", "canal"
			BinlogParserManager.startDump(slaveId, binlogfilename,
					binlogPosition, serverhost, serverPort, username, password,
					resVO);
			logger.info("parseRes vo:" + resVO);
			System.out.println("----------OVER------------");
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
