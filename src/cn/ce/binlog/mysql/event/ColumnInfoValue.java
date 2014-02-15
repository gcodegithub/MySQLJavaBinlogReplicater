package cn.ce.binlog.mysql.event;

import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

@XmlAccessorType(XmlAccessType.FIELD)
public class ColumnInfoValue {
	private String columnName;
	private boolean isPrimaryKey;
	private int index;
	private boolean isNull;
	private boolean isBinary;
	private int javaType;
	private String columnType;
	private String value;
	private boolean isAfter;

	public Object getReadValue() {
		Object obj = null;
		if (value == null) {
			return value;
		}
		try {
			switch (javaType) {
			case Types.DATE:
				// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				// Date date = sdf.parse(value);
				java.sql.Date date = java.sql.Date.valueOf(value);
				obj = date;
				break;
			case Types.TIMESTAMP:
				java.sql.Timestamp datetime = java.sql.Timestamp.valueOf(value);
				obj = datetime;
				break;
			case Types.INTEGER:
				obj = new Integer(value);
				break;
			case Types.BIGINT:
				obj = new Long(value);
				break;
			case Types.DOUBLE:
				obj = new Double(value);
				break;
			case Types.FLOAT:
				obj = new Float(value);
				break;
			case Types.DECIMAL:
				obj = new Double(value);
				break;
			case Types.BOOLEAN:
				obj = new Boolean(value);
				break;
			default:
				obj = value;
				break;
			}
		} catch (Throwable ex) {
			System.err.println("ex--------:" + ex.getMessage());
			System.err.println("javaType:" + javaType);
			System.err.println("value:" + value);
			obj = value;
			ex.printStackTrace();
		}
		return obj;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public boolean isNull() {
		return isNull;
	}

	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}

	public boolean isBinary() {
		return isBinary;
	}

	public void setBinary(boolean isBinary) {
		this.isBinary = isBinary;
	}

	public int getJavaType() {
		return javaType;
	}

	public void setJavaType(int javaType) {
		this.javaType = javaType;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public boolean isAfter() {
		return isAfter;
	}

	public void setAfter(boolean isAfter) {
		this.isAfter = isAfter;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}

	public static void main(String[] args) {
		try {
			String value = "2014-02-09";

			// Date date = java.sql.Date.valueOf(value);

			System.out.println(java.sql.Timestamp.valueOf(value));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
