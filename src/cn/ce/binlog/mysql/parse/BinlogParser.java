package cn.ce.binlog.mysql.parse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.BinlogParserManager;
import cn.ce.cons.Const;
import cn.ce.utils.mail.Alarm;
import cn.ce.web.rest.vo.BinParseResultVO;

public class BinlogParser {
	private final static Log logger = LogFactory.getLog(BinlogParser.class);

	public static void startDump(final BinlogParseSession parseSession,
			final BinParseResultVO resVo) throws Throwable {
		System.out.println("---------BinlogParser Thread----------------"
				+ Thread.currentThread());
		// System.out
		// .println("重要：数据库必须绑定IP若更改数据库IP后需要同步更改Slave端IP设置，需要清除之前該SlaveId的對應檢查點設置文件");
		MysqlConnector c = parseSession.getC();
		long binlogPosition = parseSession.getLogPosition().getPosition();
		if (binlogPosition < 4) {
			throw new RuntimeException(
					"first 4 byte is 0xfe 0x62 0x69 0x6e the magic num");
		}
		if (!c.isConnected()) {
			c.connect();
		}

		MysqlConnector nc = c.clone();
		try {
			nc.reconnect();
			System.out.println("---------BinlogParser发送解析请求----------------");
			MySQLDumper.sendBinlogDump(c, parseSession);
			parseSession.setParseThread(Thread.currentThread());
			TableMetaCache tableMetaCache = new TableMetaCache(nc);
			parseSession.setTableMetaCache(tableMetaCache);
			while (c.isConnected()) {
				try {
					MySQLDumper.receiveBinlogDump(c, parseSession);
					System.out
							.println("---------BinlogParser处理完一次输入包----------------");
				} catch (SocketUnexpectedEndException ex) {
					String err = ex.getMessage();
					ex.printStackTrace();
					c.reconnect();
					Alarm.sendAlarmEmail(
							Const.sysconfigFileClasspath,
							"警告:MySQL Master网络接口超时断掉",
							err + "\n" + parseSession.toString() + "\n"
									+ resVo.toString());
				}
			}
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			err = "解析binlog线程停止，MySQL主库数据包解析失败，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n"
					+ parseSession.toString() + "\n" + resVo.toString());
		} finally {
			System.out.println("---------BinlogParser解析线程结束----------------");
			c.disconnect();
			nc.disconnect();
			parseSession.setParseThread(null);
		}
	}

	public static void main(String args[]) {
		MysqlConnector c = null;
		try {
			c = new MysqlConnector("192.168.215.1", 3306, "canal", "canal");
			String binlogfilename = "mysql-bin.000001";
			Long binlogPosition = 175L;
			Long slaveId = 111L;
			BinlogParseSession parseSession = new BinlogParseSession();
			parseSession.setC(c);
			parseSession.setSlaveId(slaveId);
			parseSession.setLogPosition(binlogfilename, binlogPosition);
			BinParseResultVO resVo = new BinParseResultVO();
			BinlogParser.startDump(parseSession, resVo);
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			if (c != null)
				c.disconnect();
		}
		System.out.println("--------OVER---------");
	}
}
