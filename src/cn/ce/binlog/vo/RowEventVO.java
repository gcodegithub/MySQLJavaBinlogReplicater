package cn.ce.binlog.vo;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.event.ColumnInfoValue;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "columnLen", "rowDMLType", "beforeColumnInfo",
		"afterColumnInfo" }, name = "RowEventVOType")
public class RowEventVO extends EventVO {
	private int columnLen;
	private String rowDMLType;
	@XmlElementWrapper(name = "beforeColumnInfoList")
	@XmlElement(name = "columnInfoValue")
	private List<ColumnInfoValue> beforeColumnInfo = new ArrayList<ColumnInfoValue>();
	@XmlElementWrapper(name = "afterColumnInfoList")
	@XmlElement(name = "columnInfoValue")
	private List<ColumnInfoValue> afterColumnInfo = new ArrayList<ColumnInfoValue>();

	public List<ColumnInfoValue> getBeforeColumnInfo() {
		return beforeColumnInfo;
	}

	public void setBeforeColumnInfo(List<ColumnInfoValue> beforeColumnInfo) {
		this.beforeColumnInfo = beforeColumnInfo;
	}

	public List<ColumnInfoValue> getAfterColumnInfo() {
		return afterColumnInfo;
	}

	public void setAfterColumnInfo(List<ColumnInfoValue> afterColumnInfo) {
		this.afterColumnInfo = afterColumnInfo;
	}

	public int getColumnLen() {
		return columnLen;
	}

	public void setColumnLen(int columnLen) {
		this.columnLen = columnLen;
	}

	public String getRowDMLType() {
		return rowDMLType;
	}

	public void setRowDMLType(String rowDMLType) {
		this.rowDMLType = rowDMLType;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
