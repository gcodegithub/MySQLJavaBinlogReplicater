package cn.ce.binlog.vo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.event.ColumnInfoValue;

public class SQLRowVO {
	private String dbname;
	private String tablename;
	private Integer primaryKeyIndex;
	private List<ColumnInfoValue> rowValueInfo = new ArrayList<ColumnInfoValue>(
			50);
	private String dmlType;
	private long when;

	public long getWhen() {
		return when;
	}

	public void setWhen(long when) {
		this.when = when;
	}

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public String getDmlType() {
		return dmlType;
	}

	public void setDmlType(String dmlType) {
		this.dmlType = dmlType;
	}

	public List<ColumnInfoValue> getRowValueInfo() {
		return rowValueInfo;
	}

	public void addRowValueInfo(ColumnInfoValue cellValue) {
		this.rowValueInfo.add(cellValue);
	}

	public Integer getPrimaryKeyIndex() {
		return primaryKeyIndex;
	}

	public void setPrimaryKeyIndex(Integer primaryKeyIndex) {
		this.primaryKeyIndex = primaryKeyIndex;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
