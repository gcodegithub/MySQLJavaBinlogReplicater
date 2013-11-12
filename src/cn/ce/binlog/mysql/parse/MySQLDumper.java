package cn.ce.binlog.mysql.parse;

import java.io.IOException;
import java.nio.ByteBuffer;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.pack.BinlogDumpComReqPacket;
import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.pack.HeaderPacket;
import cn.ce.binlog.mysql.util.ReadWriteUtil;
import cn.ce.binlog.session.BinlogParseSession;

public class MySQLDumper {
	private final static Log logger = LogFactory.getLog(MySQLDumper.class);

	public static void sendBinlogDump(MysqlConnector con,
			BinlogParseSession session) throws IOException {
		logger.info("sendBinlogDump");
		String binlogfilename = session.getLogPosition().getFileName();
		Long binlogPosition = session.getLogPosition().getPosition();
		long slaveId = session.getSlaveId();
		BinlogDumpComReqPacket binlogDumpCmd = new BinlogDumpComReqPacket(
				binlogfilename, binlogPosition, slaveId);
		logger.info("COM_BINLOG_DUMP: " + binlogDumpCmd);
		ReadWriteUtil.write(
				con.getChannel(),
				new ByteBuffer[] {
						ByteBuffer.wrap(binlogDumpCmd.getBinlogDumpHeader()
								.toBytes()),
						ByteBuffer.wrap(binlogDumpCmd.toBytes()) });
	}

	public static BinlogDumpResPacket receiveBinlogDump(MysqlConnector con,
			BinlogParseSession parseSession) throws Exception {
		logger.info("-------------Receive one BinlogDumpResPacket begin");
		// 第一個包
		byte[] h = ReadWriteUtil.readBytes(con.getChannel(), 4);
		int packetBodyLength = (h[0] & 0xFF) | ((h[1] & 0xFF) << 8)
				| ((h[2] & 0xFF) << 16);
		byte seqNum = h[3];
		byte[] body = ReadWriteUtil.readBytes(con.getChannel(),
				packetBodyLength);
		boolean isMuliPack = false;
		// 检测是否多包
		if (packetBodyLength == (256 * 256 * 256 - 1)) {
			isMuliPack = true;
		}
		// 合并多个包
		while (packetBodyLength == (256 * 256 * 256 - 1)) {
			h = ReadWriteUtil.readBytesAsBuffer(con.getChannel(), 4).array();
			packetBodyLength = (h[0] & 0xFF) | ((h[1] & 0xFF) << 8)
					| ((h[2] & 0xFF) << 16);
			byte[] needAdd2Body = ReadWriteUtil.readBytes(con.getChannel(),
					packetBodyLength);
			body = ReadWriteUtil.mergeBytes(body, needAdd2Body);
			seqNum = h[3];
		}
		// 多个包后多最后一个分包
		if (isMuliPack) {
			h = ReadWriteUtil.readBytesAsBuffer(con.getChannel(), 4).array();
			packetBodyLength = (h[0] & 0xFF) | ((h[1] & 0xFF) << 8)
					| ((h[2] & 0xFF) << 16);
			byte[] needAdd2Body = ReadWriteUtil.readBytes(con.getChannel(),
					packetBodyLength);
			body = ReadWriteUtil.mergeBytes(body, needAdd2Body);
			seqNum = h[3];
		}

		HeaderPacket header = new HeaderPacket();
		header.setPacketBodyLength(packetBodyLength);
		header.setPacketSequenceNumber(seqNum);
		//
		BinlogDumpResPacket binlogDumpRes = new BinlogDumpResPacket(header,
				body);
		binlogDumpRes.genEvent(parseSession);
		//
		logger.info("-------------Receive one BinlogDumpResPacket end");
		return binlogDumpRes;
	}

	public static void main(String args[]) {
		BinlogParseSession parseSession = new BinlogParseSession();
		MysqlConnector c = null;
		try {
			c = new MysqlConnector("192.168.215.1", 3306, "canal", "canal");
			c.connect();

			parseSession.setLogPosition("mysql-bin.000001", 4L);
			parseSession.setSlaveId(111L);
			MySQLDumper.sendBinlogDump(c, parseSession);
			while (true) {
				MySQLDumper.receiveBinlogDump(c, parseSession);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			c.disconnect();
		}
		System.out.println("--------OVER---------");
	}
}
