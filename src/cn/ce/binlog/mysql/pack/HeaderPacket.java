package cn.ce.binlog.mysql.pack;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HeaderPacket implements IPacket {
	private final static Log logger = LogFactory.getLog(HeaderPacket.class);
	private int packetBodyLength;
	private int packetSequenceNumber;

	public byte[] toBytes() {
		byte[] data = new byte[4];
		// int 4字节32位
		// 0-2 3字节表示Packet body length
		// 大尾存放包长度
		data[0] = (byte) (packetBodyLength & 0xFF);
		data[1] = (byte) (packetBodyLength >>> 8);
		data[2] = (byte) (packetBodyLength >>> 16);
		data[3] = (byte) (packetSequenceNumber & 0xFF);
		return data;
	}

	public void fromBytes(byte[] data) throws Exception {
		if (data == null || data.length != 4) {
			throw new IllegalArgumentException(
					"invalid header data. It can't be null and the length must be 4 byte.");
		}
		this.packetBodyLength = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8)
				| ((data[2] & 0xFF) << 16);
		this.setPacketSequenceNumber(data[3]);

	}

	public int getPacketBodyLength() {
		return packetBodyLength;
	}

	public void setPacketBodyLength(int packetBodyLength) {
		this.packetBodyLength = packetBodyLength;
	}

	public void setPacketSequenceNumber(byte packetSequenceNumber) {
		this.packetSequenceNumber = packetSequenceNumber;
	}

	public int getPacketSequenceNumber() {
		return packetSequenceNumber;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}

	public static void main(String[] args) {
		Integer length = 0x000000FF;

//		System.out.println(Byte.toString((byte) (length & 0xFF)));
//		System.out.println(Integer.toHexString(length >>> 8));
		HeaderPacket hp = new HeaderPacket();
		hp.setPacketBodyLength(length);
		byte[] len = hp.toBytes();// 低位[1, 2, 3, 0]高位
//		System.out.println(len);
	}
}
