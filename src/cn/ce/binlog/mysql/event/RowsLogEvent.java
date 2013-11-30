package cn.ce.binlog.mysql.event;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.event.TableMapLogEvent.ColumnInfo;
import cn.ce.binlog.mysql.query.TableMeta;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.binlog.mysql.query.TableMeta.FieldMeta;
import cn.ce.binlog.mysql.util.MySQLColumnUtil;
import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.LogBuffer;
import cn.ce.web.rest.vo.EventVO;
import cn.ce.web.rest.vo.RowEventVO;

public abstract class RowsLogEvent extends BinlogEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 839655811118949602L;

	private Charset charset = Charset.defaultCharset();

	private final List<ColumnInfoValue> beforeColumnInfo = new ArrayList<ColumnInfoValue>();
	private final List<ColumnInfoValue> afterColumnInfo = new ArrayList<ColumnInfoValue>();
	private final long tableId; /* Table ID */
	private TableMapLogEvent table; /* The table the rows belong to */

	/** Bitmap denoting columns available */
	protected final int columnLen;
	protected final BitSet columns;
	protected final String rowDMLType;
	/**
	 * Bitmap for columns available in the after image, if present. These fields
	 * are only available for Update_rows events. Observe that the width of both
	 * the before image COLS vector and the after image COLS vector is the same:
	 * the number of columns of the table on the master.
	 */
	protected final BitSet changeColumns;

	/** XXX: Don't handle buffer in another thread. */
	private final LogBuffer rowsBuf; /* The rows in packed format */

	/**
	 * enum enum_flag
	 * 
	 * These definitions allow you to combine the flags into an appropriate flag
	 * set using the normal bitwise operators. The implicit conversion from an
	 * enum-constant to an integer is accepted by the compiler, which is then
	 * used to set the real set of flags.
	 */
	private final int flags;

	/** Last event of a statement */
	public static final int STMT_END_F = 1;

	/** Value of the OPTION_NO_FOREIGN_KEY_CHECKS flag in thd->options */
	public static final int NO_FOREIGN_KEY_CHECKS_F = (1 << 1);

	/** Value of the OPTION_RELAXED_UNIQUE_CHECKS flag in thd->options */
	public static final int RELAXED_UNIQUE_CHECKS_F = (1 << 2);

	/**
	 * Indicates that rows in this event are complete, that is contain values
	 * for all columns of the table.
	 */
	public static final int COMPLETE_ROWS_F = (1 << 3);

	/* RW = "RoWs" */
	public static final int RW_MAPID_OFFSET = 0;
	public static final int RW_FLAGS_OFFSET = 6;
	public static final int RW_VHLEN_OFFSET = 8;
	public static final int RW_V_TAG_LEN = 1;
	public static final int RW_V_EXTRAINFO_TAG = 0;

	public EventVO genEventVo() {
		RowEventVO vo = new RowEventVO();
		vo.setLogPos(header.getLogPos());
		vo.setBinfile(header.getBinlogfilename());
		vo.setMysqlServerId(header.getServerId());
		vo.setWhen(header.getWhen());
		vo.setAfterColumnInfo(afterColumnInfo);
		vo.setBeforeColumnInfo(beforeColumnInfo);
		vo.setColumnLen(columnLen);
		vo.setRowDMLType(rowDMLType);
		vo.setEventTypeString(this.getTypeName(this.getHeader().getType()));
		return vo;
	}

	public RowsLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		final int postHeaderLen = descriptionEvent.postHeaderLen[header
				.getType() - 1];
		int headerLen = 0;
		buffer.position(commonHeaderLen + RW_MAPID_OFFSET);
		if (postHeaderLen == 6) {
			/*
			 * Master is of an intermediate source tree before 5.1.4. Id is 4
			 * bytes
			 */
			tableId = buffer.getUint32();
		} else {
			tableId = buffer.getUlong48(); // RW_FLAGS_OFFSET
		}
		flags = buffer.getUint16();

		if (postHeaderLen == FormatDescriptionLogEvent.ROWS_HEADER_LEN_V2) {
			headerLen = buffer.getUint16();
			headerLen -= 2;
			int start = buffer.position();
			int end = start + headerLen;
			for (int i = start; i < end;) {
				switch (buffer.getUint8(i++)) {
				case RW_V_EXTRAINFO_TAG:
					// int infoLen = buffer.getUint8();
					buffer.position(i + EXTRA_ROW_INFO_LEN_OFFSET);
					int checkLen = buffer.getUint8(); // EXTRA_ROW_INFO_LEN_OFFSET
					int val = checkLen - EXTRA_ROW_INFO_HDR_BYTES;
					assert (buffer.getUint8() == val); // EXTRA_ROW_INFO_FORMAT_OFFSET
					for (int j = 0; j < val; j++) {
						assert (buffer.getUint8() == val); // EXTRA_ROW_INFO_HDR_BYTES
						// + i
					}
					break;
				default:
					i = end;
					break;
				}
			}
		}

		buffer.position(commonHeaderLen + postHeaderLen + headerLen);
		columnLen = (int) buffer.getPackedLong();
		columns = buffer.getBitmap(columnLen);

		if (header.getType() == UPDATE_ROWS_EVENT_V1
				|| header.getType() == UPDATE_ROWS_EVENT) {
			changeColumns = buffer.getBitmap(columnLen);
		} else {
			changeColumns = columns;
		}

		// XXX: Don't handle buffer in another thread.
		int dataSize = buffer.limit() - buffer.position();
		rowsBuf = buffer.duplicate(dataSize);
		//
		rowDMLType = genDMLType();
	}

	public final void fillTable(BinlogParseSession context) throws Exception {
		table = context.getTable(tableId);

		// end of statement check:
		if ((flags & RowsLogEvent.STMT_END_F) != 0) {
			// Now is safe to clear ignored map (clear_tables will also
			// delete original table map events stored in the map).
			context.clearAllTables();
		}
	}

	public final long getTableId() {
		return tableId;
	}

	public final TableMapLogEvent getTable() {
		return table;
	}

	public final BitSet getColumns() {
		return columns;
	}

	public final BitSet getChangeColumns() {
		return changeColumns;
	}

	public final RowsLogBuffer getRowsBuf(String charsetName) {
		return new RowsLogBuffer(rowsBuf.duplicate(), columnLen, charsetName);
	}

	public final int getFlags(final int flags) {
		return this.flags & flags;
	}

	// public int getEffectRowNum() {
	// return effectRowNum;
	// }
	//
	// public void setEffectRowNum(int effectRowNum) {
	// this.effectRowNum = effectRowNum;
	// }

	public List<ColumnInfoValue> getBeforeColumnInfo() {
		return beforeColumnInfo;
	}

	public List<ColumnInfoValue> getAfterColumnInfo() {
		return afterColumnInfo;
	}

	public String getRowDMLType() {
		return rowDMLType;
	}

	public void genColumInfo(BinlogParseSession context) throws Exception {
		TableMetaCache tableMetaCache = context.getTableMetaCache();
		try {
			TableMapLogEvent table = this.getTable();
			if (table == null) {
				// tableId对应的记录不存在
				throw new Exception("not found tableId:" + this.getTableId());
			}
			String fullname = table.getDbName() + "." + table.getTableName();
			// long tableId = this.getTableId();

			RowsLogBuffer buffer = this.getRowsBuf(charset.name());
			BitSet columns = this.getColumns();
			BitSet changeColumns = this.getColumns();
			TableMeta tableMeta = null;
			if (tableMetaCache != null) {// 入错存在table meta cache
				tableMeta = tableMetaCache.getTableMeta(table.getDbName(),
						table.getTableName());
				if (tableMeta == null) {
					throw new Exception("not found [" + fullname
							+ "] in db , pls check!");
				}
			} else {
				// tableMetaCache == null
				throw new Exception("tableMetaCache is null, pls check!");
			}
			// nextOneRow让position++
			while (buffer.nextOneRow(columns)) {
				// 处理row记录
				if ("INSERT".equals(rowDMLType)) {
					// insert的记录放在before字段中
					parseOneRow(tableMetaCache, buffer, columns, true,
							tableMeta);
				} else if ("DELETE".equals(rowDMLType)) {
					// delete的记录放在before字段中
					parseOneRow(tableMetaCache, buffer, columns, false,
							tableMeta);
				} else {
					// update需要处理before/after
					parseOneRow(tableMetaCache, buffer, columns, false,
							tableMeta);
					if (!buffer.nextOneRow(changeColumns)) {
						break;
					}
					parseOneRow(tableMetaCache, buffer,
							this.getChangeColumns(), true, tableMeta);
				}
			}
		} catch (Exception e) {
			String err = e.getMessage();
			throw new Exception("parse row data failed.err:" + err, e);
		}

	}

	private void parseOneRow(TableMetaCache tableMetaCache,
			RowsLogBuffer buffer, BitSet cols, boolean isAfter,
			TableMeta tableMeta) throws Exception {
		final int columnCnt = this.getTable().getColumnCnt();
		final ColumnInfo[] columnInfo = this.getTable().getColumnInfo();
		// check table fileds count，只能处理加字段
		if (tableMeta != null
				&& columnInfo.length > tableMeta.getFileds().size()) {
			// online ddl增加字段操作步骤：
			// 1. 新增一张临时表，将需要做ddl表的数据全量导入
			// 2. 在老表上建立I/U/D的trigger，增量的将数据插入到临时表
			// 3. 锁住应用请求，将临时表rename为老表的名字，完成增加字段的操作
			// 尝试做一次reload，可能因为ddl没有正确解析，或者使用了类似online ddl的操作
			// 因为online ddl没有对应表名的alter语法，所以不会有clear cache的操作
			tableMeta = tableMetaCache.getTableMeta(
					this.getTable().getDbName(),
					this.getTable().getTableName(), true);// 
			if (tableMeta == null) {
				throw new Exception("not found [" + this.getTable().getDbName()
						+ "." + this.getTable().getTableName()
						+ "] in db , pls check!");
			}

			// 在做一次判断
			if (tableMeta != null
					&& columnInfo.length > tableMeta.getFileds().size()) {
				throw new Exception("column size is not match for table:"
						+ tableMeta.getFullName() + "," + columnInfo.length
						+ " vs " + tableMeta.getFileds().size());
			}
		}

		for (int i = 0; i < columnCnt; i++) {
			ColumnInfoValue columnValue = new ColumnInfoValue();
			ColumnInfo info = columnInfo[i];
			FieldMeta fieldMeta = null;
			if (tableMeta != null) {
				// 处理file meta
				fieldMeta = tableMeta.getFileds().get(i);
				columnValue.setColumnName(fieldMeta.getColumnName());
				columnValue.setPrimaryKey(fieldMeta.isKey());
			}
			columnValue.setIndex(i);
			columnValue.setNull(false);

			// fixed issue
			// https://github.com/alibaba/canal/issues/66，特殊处理binary/varbinary，不能做编码处理
			boolean isBinary = false;
			if (fieldMeta != null) {
				if (StringUtils.containsIgnoreCase(fieldMeta.getColumnType(),
						"VARBINARY")) {
					isBinary = true;
				} else if (StringUtils.containsIgnoreCase(
						fieldMeta.getColumnType(), "BINARY")) {
					isBinary = true;
				}
			}
			columnValue.setBinary(isBinary);
			buffer.nextValue(info.type, info.meta, isBinary);

			int javaType = buffer.getJavaType();
			if (buffer.isNull()) {
				columnValue.setNull(true);
			} else {
				final Serializable value = buffer.getValue();
				// 处理各种类型
				switch (javaType) {
				case Types.INTEGER:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.BIGINT:
					// 处理unsigned类型
					Number number = (Number) value;
					if (fieldMeta != null && fieldMeta.isUnsigned()
							&& number.longValue() < 0) {
						switch (buffer.getLength()) {
						case 1: /* MYSQL_TYPE_TINY */
							columnValue.setValue(String.valueOf(Integer
									.valueOf(MySQLColumnUtil.TINYINT_MAX_VALUE
											+ number.intValue())));
							javaType = Types.SMALLINT; // 往上加一个量级
							break;

						case 2: /* MYSQL_TYPE_SHORT */
							columnValue.setValue(String.valueOf(Integer
									.valueOf(MySQLColumnUtil.SMALLINT_MAX_VALUE
											+ number.intValue())));
							javaType = Types.INTEGER; // 往上加一个量级
							break;

						case 3: /* MYSQL_TYPE_INT24 */
							columnValue
									.setValue(String.valueOf(Integer
											.valueOf(MySQLColumnUtil.MEDIUMINT_MAX_VALUE
													+ number.intValue())));
							javaType = Types.INTEGER; // 往上加一个量级
							break;

						case 4: /* MYSQL_TYPE_LONG */
							columnValue.setValue(String.valueOf(Long
									.valueOf(MySQLColumnUtil.INTEGER_MAX_VALUE
											+ number.longValue())));
							javaType = Types.BIGINT; // 往上加一个量级
							break;

						case 8: /* MYSQL_TYPE_LONGLONG */
							columnValue
									.setValue(MySQLColumnUtil.BIGINT_MAX_VALUE
											.add(BigInteger.valueOf(number
													.longValue())).toString());
							javaType = Types.DECIMAL; // 往上加一个量级，避免执行出错
							break;
						}
					} else {
						// 对象为number类型，直接valueof即可
						columnValue.setValue(String.valueOf(value));
					}
					break;
				case Types.REAL: // float
				case Types.DOUBLE: // double
					// 对象为number类型，直接valueof即可
					columnValue.setValue(String.valueOf(value));
					break;
				case Types.BIT:// bit
					// 对象为number类型
					columnValue.setValue(String.valueOf(value));
					break;
				case Types.DECIMAL:
					columnValue.setValue(((BigDecimal) value).toPlainString());
					break;
				case Types.TIMESTAMP:
					// 修复时间边界值
					// String v = value.toString();
					// v = v.substring(0, v.length() - 2);
					// columnBuilder.setValue(v);
					// break;
				case Types.TIME:
				case Types.DATE:
					// 需要处理year
					columnValue.setValue(value.toString());
					break;
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
					// fixed text encoding
					// https://github.com/AlibabaTech/canal/issues/18
					// mysql binlog中blob/text都处理为blob类型，需要反查table
					// meta，按编码解析text
					if (fieldMeta != null
							&& MySQLColumnUtil
									.isText(fieldMeta.getColumnType())) {
						columnValue
								.setValue(new String((byte[]) value, charset));
						javaType = Types.CLOB;
					} else {
						// byte数组，直接使用iso-8859-1保留对应编码，浪费内存
						columnValue.setValue(new String((byte[]) value,
								"ISO_8859_1"));
						javaType = Types.BLOB;
					}
					break;
				case Types.CHAR:
				case Types.VARCHAR:
					columnValue.setValue(value.toString());
					break;
				default:
					columnValue.setValue(value.toString());
				}

			}

			columnValue.setJavaType(javaType);
			// 设置是否update的标记位
			columnValue.setAfter(isAfter);
			if (isAfter) {
				afterColumnInfo.add(columnValue);
			} else {
				beforeColumnInfo.add(columnValue);
			}
		}

	}

	private String genDMLType() {
		int type = this.getHeader().getType();
		if (BinlogEvent.WRITE_ROWS_EVENT_V1 == type
				|| BinlogEvent.WRITE_ROWS_EVENT == type) {
			return "INSERT";
		} else if (BinlogEvent.UPDATE_ROWS_EVENT_V1 == type
				|| BinlogEvent.UPDATE_ROWS_EVENT == type) {

			return "UPDATE";
		} else if (BinlogEvent.DELETE_ROWS_EVENT_V1 == type
				|| BinlogEvent.DELETE_ROWS_EVENT == type) {

			return "DELETE";
		} else {
			throw new RuntimeException("unsupport event type :"
					+ this.getHeader().getType());
		}
	}
}
