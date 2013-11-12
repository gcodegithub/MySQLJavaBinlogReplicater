package cn.ce.binlog.mysql.pack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.util.ReadWriteUtil;

public class BinlogDumpComReqPacket extends MySQLCommandPacket {

	private long binlogPosition;
	private long slaveServerId;
	private String binlogFileName;
	private HeaderPacket binlogDumpHeader;
	private byte[] packBody;

	public BinlogDumpComReqPacket(String binlogFileName, long binlogPosition,
			long slaveServerId) throws IOException {
		setCommand((byte) 0x12);
		this.binlogFileName = binlogFileName;
		this.binlogPosition = binlogPosition;
		this.slaveServerId = slaveServerId;
		this.packBody = this.toBytes();
		this.binlogDumpHeader = new HeaderPacket();
		this.binlogDumpHeader.setPacketBodyLength(packBody.length);
		this.binlogDumpHeader.setPacketSequenceNumber((byte) 0x00);
	}

	public void fromBytes(byte[] data) {
		throw new RuntimeException("not needed");
	}

	/**
	 * <pre>
	 * Bytes                        Name
	 *  -----                        ----
	 *  1                            command
	 *  n                            arg
	 *  --------------------------------------------------------
	 *  Bytes                        Name
	 *  -----                        ----
	 *  4                            binlog position to start at (little endian)
	 *  2                            binlog flags (currently not used; always 0)
	 *  4                            server_id of the slave (little endian)
	 *  n                            binlog file name (optional)
	 * 
	 * </pre>
	 */
	public byte[] toBytes() throws IOException {
		if (packBody != null) {
			return packBody;
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		// 0. write command number
		out.write(getCommand());
		// 1. write 4 bytes bin-log position to start at
		ReadWriteUtil.writeUnsignedIntLittleEndian(binlogPosition, out);
		// 2. write 2 bytes bin-log flags
		out.write(0x00);
		out.write(0x00);
		// 3. write 4 bytes server id of the slave
		ReadWriteUtil.writeUnsignedIntLittleEndian(this.slaveServerId, out);
		// 4. write bin-log file name if necessary
		if (StringUtils.isNotEmpty(this.binlogFileName)) {
			out.write(this.binlogFileName.getBytes());
		}
		return out.toByteArray();
	}

	public long getBinlogPosition() {
		return binlogPosition;
	}

	public void setBinlogPosition(long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public long getSlaveServerId() {
		return slaveServerId;
	}

	public void setSlaveServerId(long slaveServerId) {
		this.slaveServerId = slaveServerId;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	public void setBinlogFileName(String binlogFileName) {
		this.binlogFileName = binlogFileName;
	}

	public HeaderPacket getBinlogDumpHeader() {
		return binlogDumpHeader;
	}

	public void setBinlogDumpHeader(HeaderPacket binlogDumpHeader) {
		this.binlogDumpHeader = binlogDumpHeader;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
