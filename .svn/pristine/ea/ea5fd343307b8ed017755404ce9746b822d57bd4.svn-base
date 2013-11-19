package cn.ce.binlog.mysql.conv;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.RotateLogEvent;
import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.EventVO;

public class MySQLEventConsumer {
	private final static Log logger = LogFactory
			.getLog(MySQLEventConsumer.class);

	// event内容全部保存到文件中
	// public static void event2File(BinlogParseSession parseSession,
	// BinParseResultVO resVo, boolean isNeedWait) throws Exception {
	// int len = parseSession.getEventVOQueueSize();
	// for (int i = 0; i < len; i++) {
	// BinlogEvent e = parseSession.getEventVOQueue();
	// boolean isDrop = MySQLEventConsumer.isTheClazz(e.getClass());
	// if (isDrop) {
	// continue;
	// }
	// EventVO vo = e.genEventVo();
	// resVo.addEventVOList(vo);
	// // binfile记录
	// if (e instanceof RotateLogEvent) {
	// String binfilename = ((RotateLogEvent) e).getFilename();
	// resVo.setBinlogfilename(binfilename);
	// if (StringUtils.isBlank(binfilename)) {
	// throw new Exception("包出现解析错误，RotateLogEvent没有binfilename："
	// + binfilename);
	// }
	// }
	// // pos 记录
	// if (e.getLogPos() >= 4L) {
	// Long pos = e.getLogPos();
	// resVo.setBinlogPosition(pos);
	// }
	//
	// }
	// }

	public static void event2vo(BinlogParseSession parseSession,
			BinParseResultVO resVo, boolean isNeedWait) throws Exception {
		if (isNeedWait) {
			MySQLEventConsumer.waitForData(parseSession);
		}
		int len = parseSession.getEventVOQueueSize();
		for (int i = 0; i < len; i++) {
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
