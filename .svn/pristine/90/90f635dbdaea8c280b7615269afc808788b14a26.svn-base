package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public class LoadLogEvent extends BinlogEvent {
	private String table;
	private String db;
	private String fname;
	private int skipLines;
	private int numFields;
	private String[] fields;

	/* sql_ex_info */
	private String fieldTerm;
	private String lineTerm;
	private String lineStart;
	private String enclosed;
	private String escaped;
	private int optFlags;
	private int emptyFlags;

	private long execTime;

	/* Load event post-header */
	public static final int L_THREAD_ID_OFFSET = 0;
	public static final int L_EXEC_TIME_OFFSET = 4;
	public static final int L_SKIP_LINES_OFFSET = 8;
	public static final int L_TBL_LEN_OFFSET = 12;
	public static final int L_DB_LEN_OFFSET = 13;
	public static final int L_NUM_FIELDS_OFFSET = 14;
	public static final int L_SQL_EX_OFFSET = 18;
	public static final int L_DATA_OFFSET = FormatDescriptionLogEvent.LOAD_HEADER_LEN;

	/*
	 * These are flags and structs to handle all the LOAD DATA INFILE options
	 * (LINES TERMINATED etc). DUMPFILE_FLAG is probably useless (DUMPFILE is a
	 * clause of SELECT, not of LOAD DATA).
	 */
	public static final int DUMPFILE_FLAG = 0x1;
	public static final int OPT_ENCLOSED_FLAG = 0x2;
	public static final int REPLACE_FLAG = 0x4;
	public static final int IGNORE_FLAG = 0x8;

	public static final int FIELD_TERM_EMPTY = 0x1;
	public static final int ENCLOSED_EMPTY = 0x2;
	public static final int LINE_TERM_EMPTY = 0x4;
	public static final int LINE_START_EMPTY = 0x8;
	public static final int ESCAPED_EMPTY = 0x10;

	public LoadLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int loadHeaderLen = FormatDescriptionLogEvent.LOAD_HEADER_LEN;
		/*
		 * I (Guilhem) manually tested replication of LOAD DATA INFILE for
		 * 3.23->5.0, 4.0->5.0 and 5.0->5.0 and it works.
		 */
		copyLogEvent(buffer, ((header.getType() == LOAD_EVENT) ? loadHeaderLen
				+ descriptionEvent.commonHeaderLen : loadHeaderLen
				+ FormatDescriptionLogEvent.LOG_EVENT_HEADER_LEN),
				descriptionEvent);
	}

	/**
	 * @see mysql-5.1.60/sql/log_event.cc - Load_log_event::copy_log_event
	 */
	protected final void copyLogEvent(LogBuffer buffer, final int bodyOffset,
			FormatDescriptionLogEvent descriptionEvent) {
		/* this is the beginning of the post-header */
		buffer.position(descriptionEvent.commonHeaderLen + L_EXEC_TIME_OFFSET);

		execTime = buffer.getUint32(); // L_EXEC_TIME_OFFSET
		skipLines = (int) buffer.getUint32(); // L_SKIP_LINES_OFFSET
		final int tableNameLen = buffer.getUint8(); // L_TBL_LEN_OFFSET
		final int dbLen = buffer.getUint8(); // L_DB_LEN_OFFSET
		numFields = (int) buffer.getUint32(); // L_NUM_FIELDS_OFFSET

		buffer.position(bodyOffset);
		/*
		 * Sql_ex.init() on success returns the pointer to the first byte after
		 * the sql_ex structure, which is the start of field lengths array.
		 */
		if (header.getType() != LOAD_EVENT /* use_new_format */) {
			/*
			 * The code below assumes that buf will not disappear from under our
			 * feet during the lifetime of the event. This assumption holds true
			 * in the slave thread if the log is in new format, but is not the
			 * case when we have old format because we will be reusing net
			 * buffer to read the actual file before we write out the
			 * Create_file event.
			 */
			fieldTerm = buffer.getString();
			enclosed = buffer.getString();
			lineTerm = buffer.getString();
			lineStart = buffer.getString();
			escaped = buffer.getString();
			optFlags = buffer.getInt8();
			emptyFlags = 0;
		} else {
			fieldTerm = buffer.getFixString(1);
			enclosed = buffer.getFixString(1);
			lineTerm = buffer.getFixString(1);
			lineStart = buffer.getFixString(1);
			escaped = buffer.getFixString(1);
			optFlags = buffer.getUint8();
			emptyFlags = buffer.getUint8();

			if ((emptyFlags & FIELD_TERM_EMPTY) != 0)
				fieldTerm = null;
			if ((emptyFlags & ENCLOSED_EMPTY) != 0)
				enclosed = null;
			if ((emptyFlags & LINE_TERM_EMPTY) != 0)
				lineTerm = null;
			if ((emptyFlags & LINE_START_EMPTY) != 0)
				lineStart = null;
			if ((emptyFlags & ESCAPED_EMPTY) != 0)
				escaped = null;
		}

		final int fieldLenPos = buffer.position();
		buffer.forward(numFields);
		fields = new String[numFields];
		for (int i = 0; i < numFields; i++) {
			final int fieldLen = buffer.getUint8(fieldLenPos + i);
			fields[i] = buffer.getFixString(fieldLen + 1);
		}

		table = buffer.getFixString(tableNameLen + 1);
		db = buffer.getFixString(dbLen + 1);

		// null termination is accomplished by the caller
		final int from = buffer.position();
		final int end = from + buffer.limit();
		int found = from;
		for (; (found < end) && buffer.getInt8(found) != '\0'; found++)
			/* empty loop */;
		fname = buffer.getString(found);
		buffer.forward(1); // The + 1 is for \0 terminating fname
	}

	public final String getTable() {
		return table;
	}

	public final String getDb() {
		return db;
	}

	public final String getFname() {
		return fname;
	}

	public final int getSkipLines() {
		return skipLines;
	}

	public final String[] getFields() {
		return fields;
	}

	public final String getFieldTerm() {
		return fieldTerm;
	}

	public final String getLineTerm() {
		return lineTerm;
	}

	public final String getLineStart() {
		return lineStart;
	}

	public final String getEnclosed() {
		return enclosed;
	}

	public final String getEscaped() {
		return escaped;
	}

	public final int getOptFlags() {
		return optFlags;
	}

	public final int getEmptyFlags() {
		return emptyFlags;
	}

	public final long getExecTime() {
		return execTime;
	}
}
