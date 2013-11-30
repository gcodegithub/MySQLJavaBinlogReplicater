package cn.ce.binlog.mysql.conv;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.RotateLogEvent;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.BinlogParserManager;
import cn.ce.cons.Const;
import cn.ce.utils.JaxbContextUtil;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.mail.Alarm;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.EventVO;
import cn.ce.web.rest.vo.RotateLogEventVO;

public class MySQLEventConsumer {
	private final static Log logger = LogFactory
			.getLog(MySQLEventConsumer.class);

	private final AtomicLong atlong = new AtomicLong(0);
	private FilePresistence fp = new FilePresistence();

	public void save2File(final BinlogParseSession bps,
			final BinParseResultVO resVo) {
		MysqlConnector c = bps.getC();
		try {
			boolean isNeedWait = false;
			String slaveId = bps.getSlaveId().toString();
			while (c.isConnected()) {
				bps.setConsuInSleep(false);
				bps.setConsumerThread(Thread.currentThread());
				int len = bps.getEventVOQueueSize();
				// System.out.println("-------save2File--队列中元素个数:" + len);
				try {
					if (len > 0) {
						// System.out.println("------------有增量数据，準備解析,Thread:"
						// + Thread.currentThread());
						this.event2vo(bps, resVo, isNeedWait);
						// System.out.println("-------event2vo OVER");
						fp.FilePersis(resVo, c.getServerhost(),
								bps.getSlaveId());
						// System.out.println("-------FilePersis OVER");
						BinlogParserManager.saveCheckPoint(resVo, new Long(
								slaveId));
						// System.out.println("-------saveCheckPoint OVER");
						resVo.setEventVOList(new ArrayList<EventVO>());
					} else {
						// System.out.println("------------没有增量数据供解析准备睡觉了,Thread:"
						// + Thread.currentThread());
						bps.setConsuInSleep(true);
						Thread.sleep(10 * 1000);
						// System.out
						// .println("------------睡到自然醒，在看看有没有增量数据,Thread:"
						// + Thread.currentThread());
						bps.setConsuInSleep(false);
					}
				} catch (InterruptedException ex) {
					bps.setConsuInSleep(false);
					Thread.interrupted();
					// System.out.println("------------增量数据突然来到，好梦被打醒,Thread:"
					// + Thread.currentThread());
				}
			}// while end
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			err = "xml binlog文件持久化线程停止，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n"
					+ bps.toString() + "\n" + resVo.toString());
		} finally {
			// System.out
			// .println("---------MySQLEventConsumer持久化文件线程结束!!----------------");
			c.disconnect();
			bps.setConsumerThread(null);
		}
	}

	public void event2vo(BinlogParseSession parseSession,
			BinParseResultVO resVo, boolean isNeedWait) throws Exception {
		int len = parseSession.getEventVOQueueSize();
		if (isNeedWait && len == 0) {
			this.waitForData(parseSession);
			len = parseSession.getEventVOQueueSize();
		}

		for (int i = 1; i <= len; i++) {
			BinlogEvent e = parseSession.getEventVOQueue();
			EventVO vo = e.genEventVo();
			if (i == 1) {
				resVo.setBinlogfilenameBegin(e.getHeader().getBinlogfilename());
				resVo.setBinlogPositionBegin(e.getLogPos() == 0 ? 4 : e
						.getLogPos());
			}
			// boolean isDrop = this.isTheClazz(e.getClass());
			// if (isDrop) {
			// continue;
			// }
			resVo.addEventVOList(vo);

			String binfn = e.getHeader().getBinlogfilename();

			if (!StringUtils.isBlank(binfn)) {
				resVo.setBinlogfilenameNext(binfn);
			}
			if (e.getLogPos() >= 4L) {
				Long pos = e.getLogPos();
				resVo.setBinlogPositionNext(pos);
			}
			// binfile记录
			// if (e instanceof RotateLogEvent) {
			// String binfilename = ((RotateLogEvent) e).getFilename();
			// resVo.setBinlogfilenameNext(binfilename);
			// if (StringUtils.isBlank(binfilename)) {
			// throw new Exception("包出现解析错误，RotateLogEvent没有binfilename："
			// + binfilename);
			// }

			// }

			// pos 记录

		}
	}

	private void tooManyFilesWarn(String dirfullPath) throws Exception {
		File dir = new File(dirfullPath);
		File[] subFiles = dir.listFiles();
		Integer filenum = subFiles.length;
		String msg = "文件夹" + dirfullPath + "下文件数目:" + filenum;
		// System.out.println("------------------" + msg);
		String warnFileNum = ProFileUtil.findMsgString(
				Const.sysconfigFileClasspath,
				"consumer.toomanyfile.warnfilenum");
		Integer confiWarnNum = new Integer(warnFileNum);
		if (atlong.get() >= confiWarnNum * 2) {
			atlong.set(0l);
		}
		if (filenum > confiWarnNum && atlong.get() == 0) {
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath,
					"文件夹下文件数目超出阀值，需要关注", msg);
		}
		if (filenum > confiWarnNum) {
			atlong.incrementAndGet();
		}
	}

	private void waitForData(BinlogParseSession parseSession) throws Exception {
		String waitMillis = ProFileUtil.findMsgString(
				Const.sysconfigFileClasspath, "consumer.wait.millis");
		String tryNum = ProFileUtil.findMsgString(Const.sysconfigFileClasspath,
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

	private boolean isTheClazz(Class clazz) throws Exception {
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
			MySQLEventConsumer con = new MySQLEventConsumer();
			Object event = new String("aa");
			// System.out.println(con.isTheClazz(event.getClass()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
