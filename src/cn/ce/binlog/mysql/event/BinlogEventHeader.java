package cn.ce.binlog.mysql.event;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.mysql.util.ReadWriteUtil;

public class BinlogEventHeader implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4248858574529226294L;
	private final static Log logger = LogFactory
			.getLog(BinlogEventHeader.class);
	private byte[] eventHeaderBytes;

	private int type;

	/**
	 * The offset in the log where this event originally appeared (it is
	 * preserved in relay logs, making SHOW SLAVE STATUS able to print
	 * coordinates of the event in the master's binlog). Note: when a
	 * transaction is written by the master to its binlog (wrapped in
	 * BEGIN/COMMIT) the log_pos of all the queries it contains is the one of
	 * the BEGIN (this way, when one does SHOW SLAVE STATUS it sees the offset
	 * of the BEGIN, which is logical as rollback may occur), except the COMMIT
	 * query which has its real offset.
	 */
	private String binlogfilename;
	private long logPos;

	/**
	 * Timestamp on the master(for debugging and replication of
	 * NOW()/TIMESTAMP). It is important for queries and LOAD DATA INFILE. This
	 * is set at the event's creation time, except for Query and Load (et al.)
	 * events where this is set at the query's execution time, which guarantees
	 * good replication (otherwise, we could have a query and its event with
	 * different timestamps).
	 */
	private long when;

	/** Number of bytes written by write() function */
	private int eventLen;

	/**
	 * The master's server id (is preserved in the relay log; used to prevent
	 * from infinite loops in circular replication).
	 */
	private long serverId;

	/**
	 * Some 16 flags. See the definitions above for LOG_EVENT_TIME_F,
	 * LOG_EVENT_FORCED_ROTATE_F, LOG_EVENT_THREAD_SPECIFIC_F, and
	 * LOG_EVENT_SUPPRESS_USE_F for notes.
	 */
	private int flags;

	/**
	 * The value is set by caller of FD constructor and
	 * Log_event::write_header() for the rest. In the FD case it's propagated
	 * into the last byte of post_header_len[] at FD::write(). On the slave side
	 * the value is assigned from post_header_len[last] of the last seen FD
	 * event.
	 */
	private int checksumAlg;
	/**
	 * Placeholder for event checksum while writing to binlog.
	 */
	private long crc; // ha_checksum

	public BinlogEventHeader(final int type) {
		this.type = type;
	}

	public BinlogEventHeader(byte[] eventHeaderBytes) {
		this.eventHeaderBytes = eventHeaderBytes;
		int pos = 0;
		this.when = ReadWriteUtil.readUnsignedIntLittleEndian(eventHeaderBytes,
				pos);
		pos = pos + 4;
		this.type = 0xff & (int) eventHeaderBytes[pos];
		pos = pos + 1;
		this.serverId = ReadWriteUtil.readUnsignedIntLittleEndian(
				eventHeaderBytes, pos);
		pos = pos + 4;
		this.eventLen = (int) ReadWriteUtil.readUnsignedIntLittleEndian(
				eventHeaderBytes, pos);
		pos = pos + 4;
		this.logPos = ReadWriteUtil.readUnsignedIntLittleEndian(
				eventHeaderBytes, pos);
		pos = pos + 4;
		this.flags = ReadWriteUtil.readUnsignedShortLittleEndian(
				eventHeaderBytes, pos);
		pos = pos + 2;
	}

	public BinlogEventHeader(int type, long logPos, long when, int eventLen,
			long serverId, int flags) {
		super();
		this.type = type;
		this.logPos = logPos;
		this.when = when;
		this.eventLen = eventLen;
		this.serverId = serverId;
		this.flags = flags;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public long getLogPos() {
		return logPos;
	}

	public void setLogPos(long logPos) {
		this.logPos = logPos;
	}

	public long getWhen() {
		return when;
	}

	public void setWhen(long when) {
		this.when = when;
	}

	public int getEventLen() {
		return eventLen;
	}

	public void setEventLen(int eventLen) {
		this.eventLen = eventLen;
	}

	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public int getChecksumAlg() {
		return checksumAlg;
	}

	public void setChecksumAlg(int checksumAlg) {
		this.checksumAlg = checksumAlg;
	}

	public long getCrc() {
		return crc;
	}

	public void setCrc(long crc) {
		this.crc = crc;
	}

	public byte[] getEventHeaderBytes() {
		return eventHeaderBytes;
	}

	public void setEventHeaderBytes(byte[] eventHeaderBytes) {
		this.eventHeaderBytes = eventHeaderBytes;
	}

	public String getBinlogfilename() {
		return binlogfilename;
	}

	public void setBinlogfilename(String binlogfilename) {
		this.binlogfilename = binlogfilename;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}

}
