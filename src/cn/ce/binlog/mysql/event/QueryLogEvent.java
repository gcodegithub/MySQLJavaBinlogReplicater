package cn.ce.binlog.mysql.event;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.manager.Context;
import cn.ce.binlog.mysql.parse.SimpleDdlParser;
import cn.ce.binlog.mysql.parse.SimpleDdlParser.DdlResult;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.binlog.mysql.util.CharsetConversion;
import cn.ce.binlog.session.LogBuffer;
import cn.ce.binlog.vo.EventVO;
import cn.ce.binlog.vo.QueryLogEventVO;

/*
 * sql/log_event.h
 * class Query_log_event: public Log_event
 */

public class QueryLogEvent extends BinlogEvent {
	private final static Log logger = LogFactory.getLog(QueryLogEvent.class);
	public static final String BEGIN = "BEGIN";
	public static final String COMMIT = "COMMIT";
	public static final int MAX_SIZE_LOG_EVENT_STATUS = (1 + 4 /* type, flags2 */
			+ 1 + 8 /* type, sql_mode */
			+ 1 + 1 + 255 /* type, length, catalog */
			+ 1 + 4 /* type, auto_increment */
			+ 1 + 6 /* type, charset */
			+ 1 + 1 + 255 /* type, length, time_zone */
			+ 1 + 2 /* type, lc_time_names_number */
			+ 1 + 2 /* type, charset_database_number */
			+ 1 + 8 /* type, table_map_for_update */
			+ 1 + 4 /* type, master_data_written */
			+ 1 + 16 + 1 + 60/* type, user_len, user, host_len, host */);
	/**
	 * The maximum number of updated databases that a status of Query-log-event
	 * can carry. It can redefined within a range [1..
	 * OVER_MAX_DBS_IN_EVENT_MTS].
	 */
	public static final int MAX_DBS_IN_EVENT_MTS = 16;

	/**
	 * When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS the
	 * value of OVER_MAX_DBS_IN_EVENT_MTS is is put into the mts_accessed_dbs
	 * status.
	 */
	public static final int OVER_MAX_DBS_IN_EVENT_MTS = 254;

	public static final int SYSTEM_CHARSET_MBMAXLEN = 3;
	public static final int NAME_CHAR_LEN = 64;
	/* Field/table name length */
	public static final int NAME_LEN = (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN);

	private String user;
	private String host;

	/* using byte for query string */
	protected String query;
	protected String catalog;
	protected final String dbname;

	/** The number of seconds the query took to run on the master. */
	// The time in seconds that the statement took to execute. Only useful for
	// inspection by the DBA
	private final long execTime;
	private final int errorCode;
	private final long sessionId; /* thread_id */

	/**
	 * 'flags2' is a second set of flags (on top of those in Log_event), for
	 * session variables. These are thd->options which is & against a mask
	 * (OPTIONS_WRITTEN_TO_BIN_LOG).
	 */
	private long flags2;

	/** In connections sql_mode is 32 bits now but will be 64 bits soon */
	private long sql_mode;

	private long autoIncrementIncrement = -1;
	private long autoIncrementOffset = -1;

	private int clientCharset = -1;
	private int clientCollation = -1;
	private int serverCollation = -1;
	private String charsetName;

	private String timezone;

	public EventVO genEventVo() {
		QueryLogEventVO vo = new QueryLogEventVO();
		vo.setLogPos(header.getLogPos());
		vo.setBinfile(header.getBinlogfilename());
		vo.setMysqlServerId(header.getServerId());
		vo.setWhen(header.getWhen());
		vo.setEventTypeString(this.getTypeName(this.getHeader().getType()));
		vo.setCatalog(catalog);
		vo.setDbname(dbname);
		vo.setQuery(query);
		return vo;
	}

	public QueryLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) throws IOException {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		final int postHeaderLen = descriptionEvent.postHeaderLen[header
				.getType() - 1];
		/*
		 * We test if the event's length is sensible, and if so we compute
		 * data_len. We cannot rely on QUERY_HEADER_LEN here as it would not be
		 * format-tolerant. We use QUERY_HEADER_MINIMAL_LEN which is the same
		 * for 3.23, 4.0 & 5.0.
		 */
		if (buffer.limit() < (commonHeaderLen + postHeaderLen)) {
			throw new IOException("Query event length is too short.");
		}
		int dataLen = buffer.limit() - (commonHeaderLen + postHeaderLen);
		buffer.position(commonHeaderLen + Q_THREAD_ID_OFFSET);

		sessionId = buffer.getUint32(); // Q_THREAD_ID_OFFSET
		execTime = buffer.getUint32(); // Q_EXEC_TIME_OFFSET

		// TODO: add a check of all *_len vars
		final int dbLen = buffer.getUint8(); // Q_DB_LEN_OFFSET
		errorCode = buffer.getUint16(); // Q_ERR_CODE_OFFSET

		/*
		 * 5.0 format starts here. Depending on the format, we may or not have
		 * affected/warnings etc The remaining post-header to be parsed has
		 * length:
		 */
		int statusVarsLen = 0;
		if (postHeaderLen > QUERY_HEADER_MINIMAL_LEN) {
			statusVarsLen = buffer.getUint16(); // Q_STATUS_VARS_LEN_OFFSET
			/*
			 * Check if status variable length is corrupt and will lead to very
			 * wrong data. We could be even more strict and require data_len to
			 * be even bigger, but this will suffice to catch most corruption
			 * errors that can lead to a crash.
			 */
			if (statusVarsLen > Math.min(dataLen, MAX_SIZE_LOG_EVENT_STATUS)) {
				throw new IOException("status_vars_len (" + statusVarsLen
						+ ") > data_len (" + dataLen + ")");
			}
			dataLen -= statusVarsLen;
		}
		/*
		 * We have parsed everything we know in the post header for QUERY_EVENT,
		 * the rest of post header is either comes from older version MySQL or
		 * dedicated to derived events (e.g. Execute_load_query...)
		 */

		/* variable-part: the status vars; only in MySQL 5.0 */
		final int start = commonHeaderLen + postHeaderLen;
		final int limit = buffer.limit(); /* for restore */
		final int end = start + statusVarsLen;
		buffer.position(start).limit(end);
		unpackVariables(buffer, end);
		buffer.position(end);
		buffer.limit(limit);

		/* A 2nd variable part; this is common to all versions */
		final int queryLen = dataLen - dbLen - 1;
		dbname = buffer.getFixString(dbLen + 1);
		if (clientCharset >= 0) {
			charsetName = CharsetConversion.getJavaCharset(clientCharset);

			if ((charsetName != null) && (Charset.isSupported(charsetName))) {
				query = buffer.getFixString(queryLen, charsetName);
			} else {
				logger.warn("unsupported character set in query log: "
						+ "\n    ID = " + clientCharset + ", Charset = "
						+ CharsetConversion.getCharset(clientCharset)
						+ ", Collation = "
						+ CharsetConversion.getCollation(clientCharset));

				query = buffer.getFixString(queryLen);
			}
		} else {
			query = buffer.getFixString(queryLen);
		}
	}

	public void parseQueryEvent(Context context) {
		String queryString = this.query;
		if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
			return;
		} else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
			return;

		} else {
			// DDL语句处理
			DdlResult result = SimpleDdlParser.parse(queryString,
					this.getDbName());
			String schemaName = this.getDbName();
			if (StringUtils.isNotEmpty(result.getSchemaName())) {
				schemaName = result.getSchemaName();
			}
			String tableName = result.getTableName();
			EventType type = EventType.QUERY;
			// fixed issue https://github.com/alibaba/canal/issues/58
			if (result.getType() == EventType.ALTER
					|| result.getType() == EventType.ERASE
					|| result.getType() == EventType.CREATE
					|| result.getType() == EventType.TRUNCATE
					|| result.getType() == EventType.RENAME) { // 针对DDL类型
				type = result.getType();
				if (StringUtils.isEmpty(tableName)
						|| (result.getType() == EventType.RENAME && StringUtils
								.isEmpty(result.getOriTableName()))) {
					// 如果解析不出tableName,记录一下日志，方便bugfix，目前直接抛出异常，中断解析
					throw new RuntimeException(
							"SimpleDdlParser process query failed. pls submit issue with this queryString: "
									+ queryString
									+ " , and DdlResult: "
									+ result.toString());
					// return null;
				}
			} else if (result.getType() == EventType.INSERT
					|| result.getType() == EventType.UPDATE
					|| result.getType() == EventType.DELETE) {
				// 对外返回，保证兼容，还是返回QUERY类型，这里暂不解析tableName，所以无法支持过滤

			}
			TableMetaCache tableMetaCache = context.getTableMetaCache();
			// 更新下table meta cache
			if (tableMetaCache != null
					&& (result.getType() == EventType.ALTER
							|| result.getType() == EventType.ERASE || result
							.getType() == EventType.RENAME)) {
				logger.info("清理表元信息，因为queryString为：" + queryString);
				if (StringUtils.isNotEmpty(tableName)) {
					// 如果解析到了正确的表信息，则根据全名进行清除
					tableMetaCache.clearTableMeta(schemaName, tableName);
				} else {
					// 如果无法解析正确的表信息，则根据schema进行清除
					tableMetaCache.clearTableMetaWithSchemaName(schemaName);
				}
			}
		}
	}

	/* query event post-header */
	public static final int Q_THREAD_ID_OFFSET = 0;
	public static final int Q_EXEC_TIME_OFFSET = 4;
	public static final int Q_DB_LEN_OFFSET = 8;
	public static final int Q_ERR_CODE_OFFSET = 9;
	public static final int Q_STATUS_VARS_LEN_OFFSET = 11;
	public static final int Q_DATA_OFFSET = QUERY_HEADER_LEN;

	/* these are codes, not offsets; not more than 256 values (1 byte). */
	public static final int Q_FLAGS2_CODE = 0;
	public static final int Q_SQL_MODE_CODE = 1;

	/**
	 * Q_CATALOG_CODE is catalog with end zero stored; it is used only by MySQL
	 * 5.0.x where 0<=x<=3. We have to keep it to be able to replicate these old
	 * masters.
	 */
	public static final int Q_CATALOG_CODE = 2;
	public static final int Q_AUTO_INCREMENT = 3;
	public static final int Q_CHARSET_CODE = 4;
	public static final int Q_TIME_ZONE_CODE = 5;

	/**
	 * Q_CATALOG_NZ_CODE is catalog withOUT end zero stored; it is used by MySQL
	 * 5.0.x where x>=4. Saves one byte in every Query_log_event in binlog,
	 * compared to Q_CATALOG_CODE. The reason we didn't simply re-use
	 * Q_CATALOG_CODE is that then a 5.0.3 slave of this 5.0.x (x>=4) master
	 * would crash (segfault etc) because it would expect a 0 when there is
	 * none.
	 */
	public static final int Q_CATALOG_NZ_CODE = 6;

	public static final int Q_LC_TIME_NAMES_CODE = 7;

	public static final int Q_CHARSET_DATABASE_CODE = 8;

	public static final int Q_TABLE_MAP_FOR_UPDATE_CODE = 9;

	public static final int Q_MASTER_DATA_WRITTEN_CODE = 10;

	public static final int Q_INVOKER = 11;

	/**
	 * Q_UPDATED_DB_NAMES status variable collects of the updated databases
	 * total number and their names to be propagated to the slave in order to
	 * facilitate the parallel applying of the Query events.
	 */
	public static final int Q_UPDATED_DB_NAMES = 12;

	public static final int Q_MICROSECONDS = 13;

	private final void unpackVariables(LogBuffer buffer, final int end)
			throws IOException {
		int code = -1;
		try {
			while (buffer.position() < end) {
				switch (code = buffer.getUint8()) {
				case Q_FLAGS2_CODE:
					flags2 = buffer.getUint32();
					break;
				case Q_SQL_MODE_CODE:
					sql_mode = buffer.getLong64(); // QQ: Fix when sql_mode is
													// ulonglong
					break;
				case Q_CATALOG_NZ_CODE:
					catalog = buffer.getString();
					break;
				case Q_AUTO_INCREMENT:
					autoIncrementIncrement = buffer.getUint16();
					autoIncrementOffset = buffer.getUint16();
					break;
				case Q_CHARSET_CODE:
					// Charset: 6 byte character set flag.
					// 1-2 = character set client
					// 3-4 = collation client
					// 5-6 = collation server
					clientCharset = buffer.getUint16();
					clientCollation = buffer.getUint16();
					serverCollation = buffer.getUint16();
					break;
				case Q_TIME_ZONE_CODE:
					timezone = buffer.getString();
					break;
				case Q_CATALOG_CODE: /* for 5.0.x where 0<=x<=3 masters */
					final int len = buffer.getUint8();
					catalog = buffer.getFixString(len + 1);
					break;
				case Q_LC_TIME_NAMES_CODE:
					// lc_time_names_number = buffer.getUint16();
					buffer.forward(2);
					break;
				case Q_CHARSET_DATABASE_CODE:
					// charset_database_number = buffer.getUint16();
					buffer.forward(2);
					break;
				case Q_TABLE_MAP_FOR_UPDATE_CODE:
					// table_map_for_update = buffer.getUlong64();
					buffer.forward(8);
					break;
				case Q_MASTER_DATA_WRITTEN_CODE:
					// data_written = master_data_written = buffer.getUint32();
					buffer.forward(4);
					break;
				case Q_INVOKER:
					user = buffer.getString();
					host = buffer.getString();
					break;
				case Q_MICROSECONDS:
					// when.tv_usec= uint3korr(pos);
					buffer.forward(3);
					break;
				case Q_UPDATED_DB_NAMES:
					int mtsAccessedDbs = buffer.getUint8();
					/**
					 * Notice, the following check is positive also in case of
					 * the master's MAX_DBS_IN_EVENT_MTS > the slave's one and
					 * the event contains e.g the master's MAX_DBS_IN_EVENT_MTS
					 * db:s.
					 */
					if (mtsAccessedDbs > MAX_DBS_IN_EVENT_MTS) {
						mtsAccessedDbs = OVER_MAX_DBS_IN_EVENT_MTS;
						break;
					}
					String mtsAccessedDbNames[] = new String[mtsAccessedDbs];
					for (int i = 0; i < mtsAccessedDbs
							&& buffer.position() < end; i++) {
						int length = end - buffer.position();
						mtsAccessedDbNames[i] = buffer
								.getFixString(length < NAME_LEN ? length
										: NAME_LEN);
					}
					break;
				default:
					/*
					 * That's why you must write status vars in growing order of
					 * code
					 */
					if (logger.isDebugEnabled())
						logger.debug("Query_log_event has unknown status vars (first has code: "
								+ code + "), skipping the rest of them");
					break; // Break loop
				}
			}
		} catch (RuntimeException e) {
			throw new IOException("Read " + findCodeName(code) + " error: "
					+ e.getMessage(), e);
		}
	}

	private static final String findCodeName(final int code) {
		switch (code) {
		case Q_FLAGS2_CODE:
			return "Q_FLAGS2_CODE";
		case Q_SQL_MODE_CODE:
			return "Q_SQL_MODE_CODE";
		case Q_CATALOG_CODE:
			return "Q_CATALOG_CODE";
		case Q_AUTO_INCREMENT:
			return "Q_AUTO_INCREMENT";
		case Q_CHARSET_CODE:
			return "Q_CHARSET_CODE";
		case Q_TIME_ZONE_CODE:
			return "Q_TIME_ZONE_CODE";
		case Q_CATALOG_NZ_CODE:
			return "Q_CATALOG_NZ_CODE";
		case Q_LC_TIME_NAMES_CODE:
			return "Q_LC_TIME_NAMES_CODE";
		case Q_CHARSET_DATABASE_CODE:
			return "Q_CHARSET_DATABASE_CODE";
		case Q_TABLE_MAP_FOR_UPDATE_CODE:
			return "Q_TABLE_MAP_FOR_UPDATE_CODE";
		case Q_MASTER_DATA_WRITTEN_CODE:
			return "Q_MASTER_DATA_WRITTEN_CODE";
		case Q_UPDATED_DB_NAMES:
			return "Q_UPDATED_DB_NAMES";
		case Q_MICROSECONDS:
			return "Q_MICROSECONDS";
		}
		return "CODE#" + code;
	}

	public final String getUser() {
		return user;
	}

	public final String getHost() {
		return host;
	}

	public final String getQuery() {
		return query;
	}

	public final String getCatalog() {
		return catalog;
	}

	public final String getDbName() {
		return dbname;
	}

	/**
	 * The number of seconds the query took to run on the master.
	 */
	public final long getExecTime() {
		return execTime;
	}

	public final int getErrorCode() {
		return errorCode;
	}

	public final long getSessionId() {
		return sessionId;
	}

	public final long getAutoIncrementIncrement() {
		return autoIncrementIncrement;
	}

	public final long getAutoIncrementOffset() {
		return autoIncrementOffset;
	}

	public final String getCharsetName() {
		return charsetName;
	}

	public final String getTimezone() {
		return timezone;
	}

	/**
	 * Returns the charsetID value.
	 * 
	 * @return Returns the charsetID.
	 */
	public final int getClientCharset() {
		return clientCharset;
	}

	/**
	 * Returns the clientCollationId value.
	 * 
	 * @return Returns the clientCollationId.
	 */
	public final int getClientCollation() {
		return clientCollation;
	}

	/**
	 * Returns the serverCollationId value.
	 * 
	 * @return Returns the serverCollationId.
	 */
	public final int getServerCollation() {
		return serverCollation;
	}

	/**
	 * Returns the sql_mode value.
	 * 
	 * <p>
	 * The sql_mode variable. See the section "SQL Modes" in the MySQL manual,
	 * and see mysql_priv.h for a list of the possible flags. Currently
	 * (2007-10-04), the following flags are available:
	 * 
	 * <ul>
	 * <li>MODE_REAL_AS_FLOAT==0x1</li>
	 * <li>MODE_PIPES_AS_CONCAT==0x2</li>
	 * <li>MODE_ANSI_QUOTES==0x4</li>
	 * <li>MODE_IGNORE_SPACE==0x8</li>
	 * <li>MODE_NOT_USED==0x10</li>
	 * <li>MODE_ONLY_FULL_GROUP_BY==0x20</li>
	 * <li>MODE_NO_UNSIGNED_SUBTRACTION==0x40</li>
	 * <li>MODE_NO_DIR_IN_CREATE==0x80</li>
	 * <li>MODE_POSTGRESQL==0x100</li>
	 * <li>MODE_ORACLE==0x200</li>
	 * <li>MODE_MSSQL==0x400</li>
	 * <li>MODE_DB2==0x800</li>
	 * <li>MODE_MAXDB==0x1000</li>
	 * <li>MODE_NO_KEY_OPTIONS==0x2000</li>
	 * <li>MODE_NO_TABLE_OPTIONS==0x4000</li>
	 * <li>MODE_NO_FIELD_OPTIONS==0x8000</li>
	 * <li>MODE_MYSQL323==0x10000</li>
	 * <li>MODE_MYSQL40==0x20000</li>
	 * <li>MODE_ANSI==0x40000</li>
	 * <li>MODE_NO_AUTO_VALUE_ON_ZERO==0x80000</li>
	 * <li>MODE_NO_BACKSLASH_ESCAPES==0x100000</li>
	 * <li>MODE_STRICT_TRANS_TABLES==0x200000</li>
	 * <li>MODE_STRICT_ALL_TABLES==0x400000</li>
	 * <li>MODE_NO_ZERO_IN_DATE==0x800000</li>
	 * <li>MODE_NO_ZERO_DATE==0x1000000</li>
	 * <li>MODE_INVALID_DATES==0x2000000</li>
	 * <li>MODE_ERROR_FOR_DIVISION_BY_ZERO==0x4000000</li>
	 * <li>MODE_TRADITIONAL==0x8000000</li>
	 * <li>MODE_NO_AUTO_CREATE_USER==0x10000000</li>
	 * <li>MODE_HIGH_NOT_PRECEDENCE==0x20000000</li>
	 * <li>MODE_NO_ENGINE_SUBSTITUTION=0x40000000</li>
	 * <li>MODE_PAD_CHAR_TO_FULL_LENGTH==0x80000000</li>
	 * </ul>
	 * 
	 * All these flags are replicated from the server. However, all flags except
	 * MODE_NO_DIR_IN_CREATE are honored by the slave; the slave always
	 * preserves its old value of MODE_NO_DIR_IN_CREATE. This field is always
	 * written to the binlog.
	 */
	public final long getSqlMode() {
		return sql_mode;
	}

	/* FLAGS2 values that can be represented inside the binlog */
	public static final int OPTION_AUTO_IS_NULL = 1 << 14;
	public static final int OPTION_NOT_AUTOCOMMIT = 1 << 19;
	public static final int OPTION_NO_FOREIGN_KEY_CHECKS = 1 << 26;
	public static final int OPTION_RELAXED_UNIQUE_CHECKS = 1 << 27;

	/**
	 * The flags in thd->options, binary AND-ed with OPTIONS_WRITTEN_TO_BIN_LOG.
	 * The thd->options bitfield contains options for "SELECT". OPTIONS_WRITTEN
	 * identifies those options that need to be written to the binlog (not all
	 * do). Specifically, OPTIONS_WRITTEN_TO_BIN_LOG equals (OPTION_AUTO_IS_NULL
	 * | OPTION_NO_FOREIGN_KEY_CHECKS | OPTION_RELAXED_UNIQUE_CHECKS |
	 * OPTION_NOT_AUTOCOMMIT), or 0x0c084000 in hex. These flags correspond to
	 * the SQL variables SQL_AUTO_IS_NULL, FOREIGN_KEY_CHECKS, UNIQUE_CHECKS,
	 * and AUTOCOMMIT, documented in the "SET Syntax" section of the MySQL
	 * Manual. This field is always written to the binlog in version >= 5.0, and
	 * never written in version < 5.0.
	 */
	public final long getFlags2() {
		return flags2;
	}

	/**
	 * Returns the OPTION_AUTO_IS_NULL flag.
	 */
	public final boolean isAutoIsNull() {
		return ((flags2 & OPTION_AUTO_IS_NULL) == OPTION_AUTO_IS_NULL);
	}

	/**
	 * Returns the OPTION_NO_FOREIGN_KEY_CHECKS flag.
	 */
	public final boolean isForeignKeyChecks() {
		return ((flags2 & OPTION_NO_FOREIGN_KEY_CHECKS) != OPTION_NO_FOREIGN_KEY_CHECKS);
	}

	/**
	 * Returns the OPTION_NOT_AUTOCOMMIT flag.
	 */
	public final boolean isAutocommit() {
		return ((flags2 & OPTION_NOT_AUTOCOMMIT) != OPTION_NOT_AUTOCOMMIT);
	}

	/**
	 * Returns the OPTION_NO_FOREIGN_KEY_CHECKS flag.
	 */
	public final boolean isUniqueChecks() {
		return ((flags2 & OPTION_RELAXED_UNIQUE_CHECKS) != OPTION_RELAXED_UNIQUE_CHECKS);
	}

}
