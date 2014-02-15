package cn.ce.binlog.mysql.pack;

import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.util.ByteHelper;

public class ResultSetHeaderPacket extends HeaderPacket {

	private long columnCount;
	private long extra;

	public void fromBytes(byte[] data) throws IOException {
		int index = 0;
		byte[] colCountBytes = ByteHelper.readBinaryCodedLengthBytes(data,
				index);
		columnCount = ByteHelper.readLengthCodedBinary(colCountBytes, index);
		index += colCountBytes.length;
		if (index < data.length - 1) {
			extra = ByteHelper.readLengthCodedBinary(data, index);
		}
	}

	public byte[] toBytes() {
		return null;
	}

	public long getColumnCount() {
		return columnCount;
	}

	public void setColumnCount(long columnCount) {
		this.columnCount = columnCount;
	}

	public long getExtra() {
		return extra;
	}

	public void setExtra(long extra) {
		this.extra = extra;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
