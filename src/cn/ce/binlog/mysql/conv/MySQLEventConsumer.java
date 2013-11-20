package cn.ce.binlog.mysql.conv;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.RotateLogEvent;
import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.BinlogParserManager;
import cn.ce.cons.Const;
import cn.ce.utils.JaxbContextUtil;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.EventVO;

public class MySQLEventConsumer {
	private final static Log logger = LogFactory
			.getLog(MySQLEventConsumer.class);

	public static void save2File(final BinlogParseSession bps,
			final BinParseResultVO resVo) throws NumberFormatException,
			Exception {
		boolean isNeedWait = false;
		MysqlConnector c = bps.getC();
		String slaveId = bps.getSlaveId().toString();
		while (c.isConnected()) {
			bps.setConsumerThread(Thread.currentThread());
			int len = bps.getEventVOQueueSize();
			try {
				if (len > 0) {
					System.out.println("------------有增量数据，準備解析");
					BinlogParserManager.getVOFromSession(resVo, new Long(
							slaveId), isNeedWait);
					MySQLEventConsumer.FilePersis(resVo, c.getServerhost());
					BinlogParserManager
							.saveCheckPoint(resVo, new Long(slaveId));
					resVo.setEventVOList(new ArrayList<EventVO>());
				} else {
					System.out.println("------------没有增量数据供解析准备睡觉了");
					Thread.sleep(60 * 1000);
					System.out.println("------------睡到自然醒，在看看有没有增量数据");
				}
			} catch (InterruptedException ex) {
				Thread.interrupted();
				System.out.println("------------增量数据突然来到，好梦被打醒");
			}
		}
	}

	public static void FilePersis(BinParseResultVO resVo, String serverhost)
			throws Exception {
		if (resVo.getEventVOList().size() == 0) {
			return;
		}
		String absDirPath = ProFileUtil
				.findMsgString(Const.sysconfigFileClasspath,
						"bootstrap.mysql.vo.filepool.dir");
		absDirPath = absDirPath + "/" + serverhost;
		//
		String binfilename_end = resVo.getBinlogfilenameNext();
		Long pos_end = resVo.getBinlogPositionNext();
		String fileNameFullPath = absDirPath + "/" + binfilename_end + "_"
				+ pos_end;
		File target = new File(fileNameFullPath);
		FileUtils.deleteQuietly(target);
		FileUtils.touch(target);
		JaxbContextUtil.marshall(resVo, fileNameFullPath);
		ProFileUtil.checkIsExist(fileNameFullPath, true);

	}

	public static void event2vo(BinlogParseSession parseSession,
			BinParseResultVO resVo, boolean isNeedWait) throws Exception {
		int len = parseSession.getEventVOQueueSize();
		if (isNeedWait && len == 0) {
			MySQLEventConsumer.waitForData(parseSession);
			len = parseSession.getEventVOQueueSize();
		}

		for (int i = 1; i <= len; i++) {
			BinlogEvent e = parseSession.getEventVOQueue();
			boolean isDrop = MySQLEventConsumer.isTheClazz(e.getClass());
			if (isDrop) {
				continue;
			}
			EventVO vo = e.genEventVo();
			resVo.addEventVOList(vo);
			// binfile记录
			if (e instanceof RotateLogEvent) {
				String binfilename = ((RotateLogEvent) e).getFilename();
				resVo.setBinlogfilenameNext(binfilename);
				if (StringUtils.isBlank(binfilename)) {
					throw new Exception("包出现解析错误，RotateLogEvent没有binfilename："
							+ binfilename);
				}
			}
			// pos 记录
			if (e.getLogPos() >= 4L) {
				Long pos = e.getLogPos();
				resVo.setBinlogPositionNext(pos);
			}

		}
	}

	private static void waitForData(BinlogParseSession parseSession)
			throws Exception {
		String waitMillis = ProFileUtil.findMsgString(
				"conf/sysconfig.properties", "consumer.wait.millis");
		String tryNum = ProFileUtil.findMsgString("conf/sysconfig.properties",
				"consumer.wait.try.num");
		int len = parseSession.getEventVOQueueSize();
		int breakCount = new Integer(tryNum);
		int count = 0;
		while (len == 0) {
			if (count > breakCount) {
				break;
			}
			len = parseSession.getEventVOQueueSize();
			count++;
			Thread.sleep(new Long(waitMillis));
		}
	}

	private static boolean isTheClazz(Class clazz) throws Exception {
		boolean isDrop = false;
		String tokenFileClasspath = "conf/sysconfig.properties";
		String key = "consumer.drop.eventtype.csv";
		String dropcsv = ProFileUtil.findMsgString(tokenFileClasspath, key);
		if (StringUtils.isBlank(dropcsv)) {
			return isDrop;
		}
		String className = clazz.getSimpleName();
		List<String> dropClassName = BeanUtil.csvToList(dropcsv, ",");
		if (dropClassName.contains(className)) {
			return true;
		}
		return isDrop;
	}

	public static void main(String[] args) {
		try {
			Object event = new String("aa");
			System.out.println(MySQLEventConsumer.isTheClazz(event.getClass()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
