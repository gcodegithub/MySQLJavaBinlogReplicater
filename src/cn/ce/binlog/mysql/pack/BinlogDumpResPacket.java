package cn.ce.binlog.mysql.pack;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BinlogDumpResPacket {
	private final static Log logger = LogFactory
			.getLog(BinlogDumpResPacket.class);

	private HeaderPacket header;
	private byte[] binlogDumpBody;


	public BinlogDumpResPacket(HeaderPacket header, byte[] binlogDumpBody) {
		this.header = header;
		this.binlogDumpBody = binlogDumpBody;

	}


	public HeaderPacket getHeader() {
		return header;
	}


	public void setHeader(HeaderPacket header) {
		this.header = header;
	}


	public byte[] getBinlogDumpBody() {
		return binlogDumpBody;
	}


	public void setBinlogDumpBody(byte[] binlogDumpBody) {
		this.binlogDumpBody = binlogDumpBody;
	}


	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
