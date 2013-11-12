package cn.ce.binlog.mysql.pack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class QueryCommandPacket extends CommandPacket {

	private String queryString;

	public QueryCommandPacket() {
		setCommand((byte) 0x03);
	}

	public void fromBytes(byte[] data) throws IOException {
	}

	public byte[] toBytes() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(getCommand());
		out.write(getQueryString().getBytes("UTF-8"));// 链接建立时默认指定编码为UTF-8
		return out.toByteArray();
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getQueryString() {
		return queryString;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
