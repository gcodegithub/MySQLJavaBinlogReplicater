package cn.ce.binlog.mysql.conv;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.RotateLogEvent;
import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.parse.BinlogParser;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.EventVO;

public class MySQLEventConsumer {
	private final static Log logger = LogFactory
			.getLog(MySQLEventConsumer.class);

	public static void event2vo(BinlogParseSession parseSession,
			BinParseResultVO resVo) throws Exception {
		String waitMillis = ProFileUtil.findMsgString(
				"conf/sysconfig.properties", "consumer.wait.millis");
		String tryNum = ProFileUtil.findMsgString("conf/sysconfig.properties",
				"consumer.wait.try.num");
		int len = parseSession.getBinlogDumpResPacketQueueSize();
		int breakCount = new Integer(tryNum);
		int count = 0;
		while (len == 0) {
			if (count > breakCount) {
				break;
			}
			len = parseSession.getBinlogDumpResPacketQueueSize();
			count++;
			Thread.sleep(new Long(waitMillis));

		}

		for (int i = 0; i < len; i++) {
			BinlogDumpResPacket ResPack = parseSession.getBinlogDumpResPacket();
			List<BinlogEvent> list = ResPack.getBinlogEventList();
			String binfilename = null;
			for (BinlogEvent e : list) {
				boolean isDrop = MySQLEventConsumer.isTheClazz(e.getClass());
				if (isDrop) {
					continue;
				}
				EventVO vo = e.genEventVo();
				resVo.addEventVOList(vo);
				if (e instanceof RotateLogEvent) {
					binfilename = ((RotateLogEvent) e).getFilename();
				}
				if (!StringUtils.isBlank(binfilename)) {
					vo.setBinlogFileName(binfilename);
					resVo.setBinlogfilename(binfilename);
				}

				resVo.setBinlogPosition(e.getLogPos());
			}
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
