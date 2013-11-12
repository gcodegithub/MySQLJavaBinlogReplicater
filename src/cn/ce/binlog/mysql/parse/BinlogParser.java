package cn.ce.binlog.mysql.parse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.BinlogParserManager;
import cn.ce.web.rest.vo.BinParseResultVO;

public class BinlogParser {
	private final static Log logger = LogFactory.getLog(BinlogParser.class);

	public static void startDump(final BinlogParseSession parseSession,
			final BinParseResultVO resVo) throws Throwable {
		// System.out
		// .println("重要：数据库必须绑定IP若更改数据库IP后需要同步更改Slave端IP设置，需要清除之前該SlaveId的對應檢查點設置文件");
		MysqlConnector c = parseSession.getC();
		long binlogPosition = parseSession.getLogPosition().getPosition();
		long slaveId = parseSession.getSlaveId();
		if (binlogPosition < 4) {
			throw new RuntimeException(
					"first 4 byte is 0xfe 0x62 0x69 0x6e the magic num");
		}
		if (!c.isConnected()) {
			c.connect();
		}
		MysqlConnector nc = c.clone();
		;
		try {
			nc.connect();
			MySQLDumper.sendBinlogDump(c, parseSession);
			
			while (c.isConnected()) {
				TableMetaCache tableMetaCache = new TableMetaCache(nc);
				BinlogDumpResPacket dumpResPackage = MySQLDumper
						.receiveBinlogDump(c, parseSession);
				parseSession.setTableMetaCache(tableMetaCache);
				parseSession.addBinlogDumpResPacket(dumpResPackage);
				logger.info(" ######################## log filename : "
						+ parseSession.getLogPosition().getFileName()
						+ " pos : "
						+ parseSession.getLogPosition().getPosition());
			}
		}  finally {
			logger.info("-----------断开链接---------------");
			c.disconnect();
			nc.disconnect();
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
			c.disconnect();
		}
		System.out.println("--------OVER---------");
	}
}
