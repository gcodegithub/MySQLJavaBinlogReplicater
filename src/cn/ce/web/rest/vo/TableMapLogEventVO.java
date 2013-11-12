package cn.ce.web.rest.vo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "dbname", "tblname", "tableId", "columnCnt" }, name = "TableMapLogEventVOType")
public class TableMapLogEventVO extends EventVO {
	private String dbname;
	private String tblname;
	private int columnCnt;
	private long tableId;

	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getTblname() {
		return tblname;
	}

	public void setTblname(String tblname) {
		this.tblname = tblname;
	}

	public int getColumnCnt() {
		return columnCnt;
	}

	public void setColumnCnt(int columnCnt) {
		this.columnCnt = columnCnt;
	}

	public long getTableId() {
		return tableId;
	}

	public void setTableId(long tableId) {
		this.tableId = tableId;
	}

}
