package cn.ce.binlog.mysql.pack;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.BinlogEventHeader;
import cn.ce.binlog.mysql.util.ReadWriteUtil;
import cn.ce.binlog.session.BinlogParseSession;

public class BinlogDumpResPacket {
	private final static Log logger = LogFactory
			.getLog(BinlogDumpResPacket.class);

	private HeaderPacket header;
	private byte[] binlogDumpBody;

	// private List<BinlogEvent> BinlogEventList = new ArrayList<BinlogEvent>();

	public BinlogDumpResPacket(HeaderPacket header, byte[] binlogDumpBody) {
		this.header = header;
		this.binlogDumpBody = binlogDumpBody;

	}

	// public List<BinlogEvent> getBinlogEventList() {
	// return BinlogEventList;
	// }

	public void genEvent(BinlogParseSession session) throws Exception {
		// 无符号数
		final int mark = ((byte) binlogDumpBody[0]) & 0xFF;
		if (mark == 254) {
			// Indicates end of stream. It's not clear when this would
			// be sent.
			logger.warn("Received EOF packet from server, apparent"
					+ " master disconnected.");
			return;
		}
		if (mark != 0) {
			this.error(binlogDumpBody);
		}
		genEvent(binlogDumpBody, session);
		//
	}

	// --------------------------------------
	// +=====================================+
	// | event | timestamp 0 : 4 |
	// | header +----------------------------+
	// | | type_code 4 : 1 |
	// | +----------------------------+
	// | | server_id 5 : 4 |
	// | +----------------------------+
	// | | event_length 9 : 4 |
	// | +----------------------------+
	// | | next_position 13 : 4 |
	// | +----------------------------+
	// | | flags 17 : 2 |
	// | +----------------------------+
	// | | extra_headers 19 : x-19 |
	// +=====================================+
	// | event | fixed part x : y |
	// | data +----------------------------+
	// | | variable part |
	// +=====================================+
	private void genEvent(byte[] packageBody, BinlogParseSession session)
			throws Exception {
		// mark占用第一字节
		// ???mark是一个package包一个还是一个event一个
		byte[] manyEventAll = new byte[packageBody.length - 1];
		System.arraycopy(packageBody, 1, manyEventAll, 0, manyEventAll.length);
		int pos = 0;
		while (pos < manyEventAll.length) {
			int eventHeaderLen = session.getDescription().getCommonHeaderLen();
			// logger.info("-----------------pos:" + pos);
			// logger.info("-----------------manyEventAll.length:"
			// + manyEventAll.length);
			// logger.info("-----------------eventHeaderLen:" + eventHeaderLen);
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
			// if (eventHeaderLen != eventLen) {
			// logger.info("有event head，有event body");
			// } else {
			// logger.info("没有event body，只有event head");
			// }
			pos = pos + eventLen;
			BinlogEvent binlogEvent = BinlogEvent.buildEvent(eventHeader,
					oneEventAll, session);
			session.addEventVOQueue(binlogEvent);
			// if (eventHeaderLen != eventLen) {
			// logger.info("event body is not null");
			// break;
			// } else {
			// logger.info("event body is null");
			// }

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

	private void error(byte[] data) throws Exception {
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

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
