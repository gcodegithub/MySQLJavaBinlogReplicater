package cn.ce.binlog.manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.BinlogEventHeader;
import cn.ce.binlog.mysql.event.FormatDescriptionLogEvent;
import cn.ce.binlog.mysql.pack.BinlogDumpComReqPacket;
import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.pack.HeaderPacket;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.util.ReadWriteUtil;
import cn.ce.cons.Const;
import cn.ce.utils.mail.Alarm;

public class BinlogDumper {

	private final static Log logger = LogFactory.getLog(BinlogDumper.class);

	// 不间断的执行dump mysql binlog任务
	public void dump(final Context context, final ProcessInfo processInfo,
			Object[] params) {
		logger.info("BinlogParser Thread Begin " + Thread.currentThread());
		String binlogfilename = context.getBinlogfilename();
		Long binlogPosition = context.getBinlogPosition();
		Long slaveId = context.getSlaveId();
		MysqlConnector c = context.getC();
		try {
			this.sendBinlogDump(c, binlogfilename, binlogPosition, slaveId);
			while (c.isConnected() && !context.isPrepareStop()) {
				try {
					BinlogDumpResPacket binlogDumpRes = BinlogDumper
							.receiveBinlogDump(c);
					BinlogDumper.genEvent(binlogDumpRes, context);
				} catch (IOException ex) {
					String err = ex.getMessage();
					ex.printStackTrace();
					logger.error("警告:MySQL Master网络接口超时断掉,程序会自动重新连接，err：" + err);
					this.reSendBinlogDump(context);
				} catch (InterruptedException ex) {
					if (context.isParseThreadStop()) {
						return;
					} else {
						this.reSendBinlogDump(context);
					}

				}
			}// while end
		} catch (ClosedByInterruptException e) {
			logger.warn("---------BinlogParser解析线程被强行终止----------------,err:"
					+ e.getMessage());
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			err = "解析binlog线程停止，MySQL主库数据包解析失败，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n"
					+ context.toString() + "\n" + processInfo.toString());
		} finally {
			c.disconnect();
			context.setPrepareStop(true);
			context.setParseThreadStop(true);
			logger.info("---------BinlogParser解析线程结束----------------");
		}

	}

	private void reSendBinlogDump(Context context) {
		while (true) {
			try {
				MysqlConnector c = context.getC();
				c.reconnect();
				String binlogfilename = context.getBinlogfilename();
				Long binlogPosition = context.getBinlogPosition();
				Long slaveId = context.getSlaveId();
				this.sendBinlogDump(c, binlogfilename, binlogPosition, slaveId);
				return;
			} catch (Throwable e) {
				String err = e.getMessage();
				logger.error("警告:MySQL Master网络接口超时断掉,程序会自动重新连接，err：" + err);
				e.printStackTrace();
			}
		}
	}

	private void sendBinlogDump(MysqlConnector c, String binlogfilename,
			Long binlogPosition, long slaveId) throws IOException {
		BinlogDumpComReqPacket binlogDumpCmd = new BinlogDumpComReqPacket(
				binlogfilename, binlogPosition, slaveId);
		// logger.info("COM_BINLOG_DUMP: " + binlogDumpCmd);
		ReadWriteUtil.write(
				c.getChannel(),
				new ByteBuffer[] {
						ByteBuffer.wrap(binlogDumpCmd.getBinlogDumpHeader()
								.toBytes()),
						ByteBuffer.wrap(binlogDumpCmd.toBytes()) });
	}

	private static BinlogDumpResPacket receiveBinlogDump(MysqlConnector con)
			throws Exception {
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
		return binlogDumpRes;
	}

	// --------------------------------------
	private static void genEvent(BinlogDumpResPacket binlogDumpRes,
			Context context) throws Exception {
		byte[] binlogDumpBody = binlogDumpRes.getBinlogDumpBody();
		// 无符号数
		final int mark = ((byte) binlogDumpBody[0]) & 0xFF;
		if (mark == 254) {
			logger.warn("Received EOF packet from server, apparent"
					+ " master disconnected.");
			return;
		}
		if (mark != 0) {
			BinlogDumper.error(binlogDumpBody);
		}
		// mark占用第一字节
		// ???mark是一个package包一个还是一个event一个
		byte[] manyEventAll = new byte[binlogDumpBody.length - 1];
		System.arraycopy(binlogDumpBody, 1, manyEventAll, 0,
				manyEventAll.length);
		int pos = 0;
		while (pos < manyEventAll.length) {
			int eventHeaderLen = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_USED
					.getCommonHeaderLen();
			// event 头内容
			byte[] eventHeaderBytes = new byte[eventHeaderLen];
			System.arraycopy(manyEventAll, pos, eventHeaderBytes, 0,
					eventHeaderLen);
			BinlogEventHeader eventHeader = new BinlogEventHeader(
					eventHeaderBytes);

			// event 长度
			int eventLen = eventHeader.getEventLen();
			int packageLenInM = eventLen / 1024 / 1024;
			if (packageLenInM > 100) {
				throw new Exception("检查点位置错误，包长度溢出，packageLenInM:"
						+ packageLenInM + "M");
			}
			byte[] oneEventAll = new byte[eventLen];
			System.arraycopy(manyEventAll, pos, oneEventAll, 0, eventLen);
			pos = pos + eventLen;
			BinlogEvent binlogEvent = BinlogEvent.buildEvent(eventHeader,
					oneEventAll, context);
			context.addEventVOQueue(binlogEvent);

		}
	}

	/*
	 * 
	 * header (1) -- ERR Packet indicator
	 * 
	 * error_code (2) -- error-code
	 * 
	 * sql_state_marker (string.fix_len) -- [len = 1], #
	 * 
	 * sql_state (string.fix_len) -- [len = 5], SQL State of this error
	 * 
	 * error_message (string.EOF) -- human readable error message
	 */

	private static void error(byte[] data) throws Exception {
		final int mark = ((byte) data[0]) & 0xFF;
		if (mark == 255) {
			// error from master
			int pos = BinlogEvent.NET_HEADER_SIZE - 1;
			// error_code (2) -- error-code
			final int errno = ReadWriteUtil.readUnsignedShortLittleEndian(data,
					pos);
			// sql_state_marker (string.fix_len) -- [len = 1], #
			// sql_state (string.fix_len) -- [len = 5], SQL State of this error
			String sqlstate = ReadWriteUtil.getFixString(data, pos + 1,
					BinlogEvent.SQLSTATE_LENGTH, "UTF-8");
			pos = pos + BinlogEvent.SQLSTATE_LENGTH + 1;
			// error_message (string.EOF) -- human readable error message
			String errmsg = ReadWriteUtil.getFixString(data, pos, data.length
					- pos, "UTF-8");
			throw new IOException("Received error packet:" + " errno = "
					+ errno + ", sqlstate = " + sqlstate + " errmsg = "
					+ errmsg);
		} else {
			// Should not happen.
			throw new IOException("Unexpected response " + mark
					+ " while fetching binlog: packet #");
		}

	}

}
