package cn.ce.binlog.mysql.query;

import java.io.Serializable;
import java.util.List;

public class TableMeta implements Serializable {

	private static final long serialVersionUID = 1370161574223864580L;
	private String fullName; // schema.table
	private List<FieldMeta> fileds;

	public TableMeta(String fullName, List<FieldMeta> fileds) {
		this.fullName = fullName;
		this.fileds = fileds;
	}

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public List<FieldMeta> getFileds() {
		return fileds;
	}

	public void setFileds(List<FieldMeta> fileds) {
		this.fileds = fileds;
	}

	public static class FieldMeta implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -123987725289591644L;
		private String columnName;
		private String columnType;
		private String isNullable;
		private String iskey;
		private String defaultValue;
		private String extra;

		public String getColumnName() {
			return columnName;
		}

		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String getColumnType() {
			return columnType;
		}

		public void setColumnType(String columnType) {
			this.columnType = columnType;
		}

		public String getIsNullable() {
			return isNullable;
		}

		public void setIsNullable(String isNullable) {
			this.isNullable = isNullable;
		}

		public String getIskey() {
			return iskey;
		}

		public void setIskey(String iskey) {
			this.iskey = iskey;
		}

		public String getDefaultValue() {
			return defaultValue;
		}

		public void setDefaultValue(String defaultValue) {
			this.defaultValue = defaultValue;
		}

		public String getExtra() {
			return extra;
		}

		public void setExtra(String extra) {
			this.extra = extra;
		}

		public boolean isUnsigned() {
			return "unsigned".equalsIgnoreCase(columnType);
		}

		public boolean isKey() {
			return "PRI".equalsIgnoreCase(iskey);
		}

		public boolean isNullable() {
			return "YES".equalsIgnoreCase(isNullable);
		}

		public String toString() {
			return "FieldMeta [columnName=" + columnName + ", columnType="
					+ columnType + ", defaultValue=" + defaultValue
					+ ", extra=" + extra + ", isNullable=" + isNullable
					+ ", iskey=" + iskey + "]";
		}

	}
}
