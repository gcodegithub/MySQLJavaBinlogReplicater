package cn.ce.binlog.mysql.pack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.util.LengthCodedStringReader;


public class RowDataPacket extends HeaderPacket {

	private List<String> columns = new ArrayList<String>();

	public void fromBytes(byte[] data) throws IOException {
		int index = 0;
		LengthCodedStringReader reader = new LengthCodedStringReader(null,
				index);
		do {
			getColumns().add(reader.readLengthCodedString(data));
		} while (reader.getIndex() < data.length);
	}

	public byte[] toBytes() {
		return null;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public List<String> getColumns() {
		return columns;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}

}
