package cn.ce.binlog.mysql.pack;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.util.ReadWriteUtil;


public class HandshakeResPacket implements IPacket {
	private byte protocolVersion = IPacket.DEFAULT_PROTOCOL_VERSION;
	private String serverVersion;
	private long threadId;
	private byte[] seed;
	private int serverCapabilities;
	private byte serverCharsetNumber;
	private int serverStatus;
	private byte[] restOfScrambleBuff;
	private HeaderPacket header;

	public HandshakeResPacket(HeaderPacket header) {
		this.header = header;
	}

	/**
	 * <pre>
	 * Bytes                        Name
	 *  -----                        ----
	 *  1                            protocol_version
	 *  n (Null-Terminated String)   server_version
	 *  4                            thread_id
	 *  8                            scramble_buff
	 *  1                            (filler) always 0x00
	 *  2                            server_capabilities
	 *  1                            server_language
	 *  2                            server_status
	 *  13                           (filler) always 0x00 ...
	 *  13                           rest of scramble_buff (4.1)
	 * </pre>
	 */
	public void fromBytes(byte[] data) {
		int index = 0;
		// 1. read protocol_version
		protocolVersion = data[index];
		index++;
		// 2. read server_version
		byte[] serverVersionBytes = ReadWriteUtil.readNullTerminatedBytes(data,
				index);
		serverVersion = new String(serverVersionBytes);
		index += (serverVersionBytes.length + 1);
		// 3. read thread_id
		threadId = ReadWriteUtil.readUnsignedIntLittleEndian(data, index);
		index += 4;
		// 4. read scramble_buff
		seed = ReadWriteUtil.readFixedLengthBytes(data, index, 8);
		index += 8;
		index += 1; // 1 byte (filler) always 0x00
		// 5. read server_capabilities
		this.serverCapabilities = ReadWriteUtil.readUnsignedShortLittleEndian(
				data, index);
		index += 2;
		// 6. read server_language
		this.serverCharsetNumber = data[index];
		index++;
		// 7. read server_status
		this.serverStatus = ReadWriteUtil.readUnsignedShortLittleEndian(data,
				index);
		index += 2;
		// 8. bypass filtered bytes
		index += 13;
		// 9. read rest of scramble_buff
		this.restOfScrambleBuff = ReadWriteUtil.readFixedLengthBytes(data,
				index, 12); // 虽然Handshake Initialization
		// Packet规定最后13个byte是剩下的scrumble,
		// 但实际上最后一个字节是0, 不应该包含在scrumble中.
		// end read
	}

	/**
	 * Bypass implementing it, 'cause nowhere to use it.
	 */
	public byte[] toBytes() throws IOException {
		return null;
	}

	public byte getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(byte protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public String getServerVersion() {
		return serverVersion;
	}

	public void setServerVersion(String serverVersion) {
		this.serverVersion = serverVersion;
	}

	public long getThreadId() {
		return threadId;
	}

	public void setThreadId(long threadId) {
		this.threadId = threadId;
	}

	public byte[] getSeed() {
		return seed;
	}

	public void setSeed(byte[] seed) {
		this.seed = seed;
	}

	public int getServerCapabilities() {
		return serverCapabilities;
	}

	public void setServerCapabilities(int serverCapabilities) {
		this.serverCapabilities = serverCapabilities;
	}

	public byte getServerCharsetNumber() {
		return serverCharsetNumber;
	}

	public void setServerCharsetNumber(byte serverCharsetNumber) {
		this.serverCharsetNumber = serverCharsetNumber;
	}

	public int getServerStatus() {
		return serverStatus;
	}

	public void setServerStatus(int serverStatus) {
		this.serverStatus = serverStatus;
	}

	public byte[] getRestOfScrambleBuff() {
		return restOfScrambleBuff;
	}

	public void setRestOfScrambleBuff(byte[] restOfScrambleBuff) {
		this.restOfScrambleBuff = restOfScrambleBuff;
	}

	public HeaderPacket getHeader() {
		return header;
	}

	public void setHeader(HeaderPacket header) {
		this.header = header;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
