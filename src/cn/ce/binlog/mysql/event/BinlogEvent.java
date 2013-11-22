package cn.ce.binlog.mysql.event;

import java.io.Serializable;
import java.util.BitSet;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.binlog.session.BinlogParseSession;
import cn.ce.binlog.session.BinlogPosition;
import cn.ce.binlog.session.LogBuffer;
import cn.ce.web.rest.vo.EventVO;

public class BinlogEvent implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4394370178638987282L;

	protected BinlogEventHeader header;

	protected static final BitSet handleSet = new BitSet(
			BinlogEvent.ENUM_END_EVENT);

	public EventVO genEventVo() {
		EventVO vo = new EventVO();
		vo.setLogPos(header.getLogPos());
		vo.setMysqlServerId(header.getServerId());
		vo.setWhen(header.getWhen());
		return vo;
	}

	protected BinlogEvent() {

	}

	protected BinlogEvent(BinlogEventHeader header) {
		this.header = header;
	}

	public static BinlogEvent buildEvent(BinlogEventHeader header,
			byte[] eventAll, BinlogParseSession context) throws Exception {
		LogBuffer buffer = new LogBuffer(eventAll, 0, eventAll.length);
		BinlogPosition logPosition = context.getLogPosition();
		FormatDescriptionLogEvent descriptionEvent = context.getDescription();
		if (!handleSet.get(header.getType())) {
			// unsupported binary-log
		}

		if (header.getType() != BinlogEvent.FORMAT_DESCRIPTION_EVENT) {
			int checksumAlg = descriptionEvent.header.getChecksumAlg();
			if (checksumAlg != BinlogEvent.BINLOG_CHECKSUM_ALG_OFF
					&& checksumAlg != BinlogEvent.BINLOG_CHECKSUM_ALG_UNDEF) {
				// remove checksum bytes
				buffer.limit(header.getEventLen()
						- BinlogEvent.BINLOG_CHECKSUM_LEN);
			}
		}

		switch (header.getType()) {
		case BinlogEvent.QUERY_EVENT: {
			logger.info("QUERY_EVENT");
			QueryLogEvent event = new QueryLogEvent(header, buffer,
					descriptionEvent);
			event.parseQueryEvent();
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			// logger.info("QueryLogEvent:" + event);
			return event;
		}
		case BinlogEvent.XID_EVENT: {
			logger.info("XID_EVENT");
			XidLogEvent event = new XidLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			// logger.info("XidLogEvent:" + event);
			return event;
		}
		case BinlogEvent.TABLE_MAP_EVENT: {
			logger.info("TABLE_MAP_EVENT");
			TableMapLogEvent mapEvent = new TableMapLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			context.putTable(mapEvent);
			return mapEvent;
		}
		case BinlogEvent.WRITE_ROWS_EVENT_V1: {
			logger.info("WRITE_ROWS_EVENT_V1");
			RowsLogEvent event = new WriteRowsLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			event.fillTable(context);
			event.genColumInfo(context);
			return event;
		}
		case BinlogEvent.UPDATE_ROWS_EVENT_V1: {
			logger.info("UPDATE_ROWS_EVENT_V1");
			RowsLogEvent event = new UpdateRowsLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			event.fillTable(context);
			event.genColumInfo(context);
			return event;
		}
		case BinlogEvent.DELETE_ROWS_EVENT_V1: {
			logger.info("DELETE_ROWS_EVENT_V1");
			RowsLogEvent event = new DeleteRowsLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			event.fillTable(context);
			event.genColumInfo(context);
			return event;
		}
		case BinlogEvent.ROTATE_EVENT: {
			logger.info("ROTATE_EVENT");
			RotateLogEvent event = new RotateLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition = new BinlogPosition(event.getFilename(),
					event.getPosition());
			context.setLogPosition(logPosition);
			// logger.info("RotateLogEvent:" + event);
			return event;
		}
		case BinlogEvent.LOAD_EVENT:
		case BinlogEvent.NEW_LOAD_EVENT: {
			logger.info("NEW_LOAD_EVENT");
			LoadLogEvent event = new LoadLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.SLAVE_EVENT: /* can never happen (unused event) */
		{
			if (logger.isWarnEnabled())
				logger.warn("Skipping unsupported SLAVE_EVENT from: "
						+ context.getLogPosition());
			break;
		}
		case BinlogEvent.CREATE_FILE_EVENT: {
			logger.info("CREATE_FILE_EVENT");
			CreateFileLogEvent event = new CreateFileLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.APPEND_BLOCK_EVENT: {
			logger.info("APPEND_BLOCK_EVENT");
			AppendBlockLogEvent event = new AppendBlockLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.DELETE_FILE_EVENT: {
			logger.info("DELETE_FILE_EVENT");
			DeleteFileLogEvent event = new DeleteFileLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.EXEC_LOAD_EVENT: {
			logger.info("EXEC_LOAD_EVENT");
			ExecuteLoadLogEvent event = new ExecuteLoadLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.START_EVENT_V3: {
			logger.info("START_EVENT_V3");
			/* This is sent only by MySQL <=4.x */
			StartLogEventV3 event = new StartLogEventV3(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.STOP_EVENT: {
			logger.info("STOP_EVENT");
			StopLogEvent event = new StopLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.INTVAR_EVENT: {
			logger.info("INTVAR_EVENT");
			IntvarLogEvent event = new IntvarLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.RAND_EVENT: {
			logger.info("RAND_EVENT");
			RandLogEvent event = new RandLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.USER_VAR_EVENT: {
			logger.info("USER_VAR_EVENT");
			UserVarLogEvent event = new UserVarLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.FORMAT_DESCRIPTION_EVENT: {
			logger.info("FORMAT_DESCRIPTION_EVENT");
			descriptionEvent = new FormatDescriptionLogEvent(header, buffer,
					descriptionEvent);
			context.setDescription(descriptionEvent);
			return descriptionEvent;
		}
		case BinlogEvent.PRE_GA_WRITE_ROWS_EVENT: {
			if (logger.isWarnEnabled())
				logger.warn("Skipping unsupported PRE_GA_WRITE_ROWS_EVENT from: "
						+ context.getLogPosition());
			break;
		}
		case BinlogEvent.PRE_GA_UPDATE_ROWS_EVENT: {
			if (logger.isWarnEnabled())
				logger.warn("Skipping unsupported PRE_GA_UPDATE_ROWS_EVENT from: "
						+ context.getLogPosition());
			break;
		}
		case BinlogEvent.PRE_GA_DELETE_ROWS_EVENT: {
			if (logger.isWarnEnabled())
				logger.warn("Skipping unsupported PRE_GA_DELETE_ROWS_EVENT from: "
						+ context.getLogPosition());
			break;
		}
		case BinlogEvent.BEGIN_LOAD_QUERY_EVENT: {
			logger.info("BEGIN_LOAD_QUERY_EVENT");
			BeginLoadQueryLogEvent event = new BeginLoadQueryLogEvent(header,
					buffer, descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.EXECUTE_LOAD_QUERY_EVENT: {
			logger.info("EXECUTE_LOAD_QUERY_EVENT");
			ExecuteLoadQueryLogEvent event = new ExecuteLoadQueryLogEvent(
					header, buffer, descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.INCIDENT_EVENT: {
			logger.info("INCIDENT_EVENT");
			IncidentLogEvent event = new IncidentLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.HEARTBEAT_LOG_EVENT: {
			logger.info("HEARTBEAT_LOG_EVENT");
			HeartbeatLogEvent event = new HeartbeatLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.IGNORABLE_LOG_EVENT: {
			logger.info("IGNORABLE_LOG_EVENT");
			IgnorableLogEvent event = new IgnorableLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.ROWS_QUERY_LOG_EVENT: {
			logger.info("ROWS_QUERY_LOG_EVENT");
			RowsQueryLogEvent event = new RowsQueryLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.WRITE_ROWS_EVENT: {
			logger.info("WRITE_ROWS_EVENT");
			RowsLogEvent event = new WriteRowsLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			event.fillTable(context);
			return event;
		}
		case BinlogEvent.UPDATE_ROWS_EVENT: {
			logger.info("UPDATE_ROWS_EVENT");
			RowsLogEvent event = new UpdateRowsLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			event.fillTable(context);
			return event;
		}
		case BinlogEvent.DELETE_ROWS_EVENT: {
			logger.info("DELETE_ROWS_EVENT");
			RowsLogEvent event = new DeleteRowsLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			event.fillTable(context);
			return event;
		}
		case BinlogEvent.GTID_LOG_EVENT:
		case BinlogEvent.ANONYMOUS_GTID_LOG_EVENT: {
			logger.info("ANONYMOUS_GTID_LOG_EVENT");
			GtidLogEvent event = new GtidLogEvent(header, buffer,
					descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		case BinlogEvent.PREVIOUS_GTIDS_LOG_EVENT: {
			logger.info("PREVIOUS_GTIDS_LOG_EVENT");
			PreviousGtidsLogEvent event = new PreviousGtidsLogEvent(header,
					buffer, descriptionEvent);
			/* updating position in context */
			logPosition.setPosition(header.getLogPos());
			return event;
		}
		default:
			/*
			 * Create an object of Ignorable_log_event for unrecognized
			 * sub-class. So that SLAVE SQL THREAD will only update the position
			 * and continue.
			 */
			if ((buffer.getUint16(BinlogEvent.FLAGS_OFFSET) & BinlogEvent.LOG_EVENT_IGNORABLE_F) > 0) {
				logger.info("LOG_EVENT_IGNORABLE_F");
				IgnorableLogEvent event = new IgnorableLogEvent(header, buffer,
						descriptionEvent);
				/* updating position in context */
				logPosition.setPosition(header.getLogPos());
				return event;
			} else {
				if (logger.isWarnEnabled())
					logger.warn("Skipping unrecognized binlog event "
							+ " from: " + context.getLogPosition());
			}
		}

		/* updating position in context */
		logPosition.setPosition(header.getLogPos());
		/* Unknown or unsupported log event */
		return new UnknownLogEvent(header);
	}

	public static final int BINLOG_VERSION = 4;

	/* Default 5.0 server version */
	public static final String SERVER_VERSION = "5.0";

	public static final int EVENT_TYPE_OFFSET = 4;
	public static final int SERVER_ID_OFFSET = 5;
	public static final int EVENT_LEN_OFFSET = 9;
	public static final int LOG_POS_OFFSET = 13;
	public static final int FLAGS_OFFSET = 17;

	/* event-specific post-header sizes */
	// where 3.23, 4.x and 5.0 agree
	public static final int QUERY_HEADER_MINIMAL_LEN = (4 + 4 + 1 + 2);
	// where 5.0 differs: 2 for len of N-bytes vars.
	public static final int QUERY_HEADER_LEN = (QUERY_HEADER_MINIMAL_LEN + 2);

	/* Enumeration type for the different types of log events. */
	public static final int UNKNOWN_EVENT = 0;
	public static final int START_EVENT_V3 = 1;
	public static final int QUERY_EVENT = 2;
	public static final int STOP_EVENT = 3;
	public static final int ROTATE_EVENT = 4;
	public static final int INTVAR_EVENT = 5;
	public static final int LOAD_EVENT = 6;
	public static final int SLAVE_EVENT = 7;
	public static final int CREATE_FILE_EVENT = 8;
	public static final int APPEND_BLOCK_EVENT = 9;
	public static final int EXEC_LOAD_EVENT = 10;
	public static final int DELETE_FILE_EVENT = 11;

	/**
	 * NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer sql_ex,
	 * allowing multibyte TERMINATED BY etc; both types share the same class
	 * (Load_log_event)
	 */
	public static final int NEW_LOAD_EVENT = 12;
	public static final int RAND_EVENT = 13;
	public static final int USER_VAR_EVENT = 14;
	public static final int FORMAT_DESCRIPTION_EVENT = 15;
	public static final int XID_EVENT = 16;
	public static final int BEGIN_LOAD_QUERY_EVENT = 17;
	public static final int EXECUTE_LOAD_QUERY_EVENT = 18;

	public static final int TABLE_MAP_EVENT = 19;

	/**
	 * These event numbers were used for 5.1.0 to 5.1.15 and are therefore
	 * obsolete.
	 */
	public static final int PRE_GA_WRITE_ROWS_EVENT = 20;
	public static final int PRE_GA_UPDATE_ROWS_EVENT = 21;
	public static final int PRE_GA_DELETE_ROWS_EVENT = 22;

	/**
	 * These event numbers are used from 5.1.16 and forward
	 */
	public static final int WRITE_ROWS_EVENT_V1 = 23;
	public static final int UPDATE_ROWS_EVENT_V1 = 24;
	public static final int DELETE_ROWS_EVENT_V1 = 25;

	/**
	 * Something out of the ordinary happened on the master
	 */
	public static final int INCIDENT_EVENT = 26;

	/**
	 * Heartbeat event to be send by master at its idle time to ensure master's
	 * online status to slave
	 */
	public static final int HEARTBEAT_LOG_EVENT = 27;

	/**
	 * In some situations, it is necessary to send over ignorable data to the
	 * slave: data that a slave can handle in case there is code for handling
	 * it, but which can be ignored if it is not recognized.
	 */
	public static final int IGNORABLE_LOG_EVENT = 28;
	public static final int ROWS_QUERY_LOG_EVENT = 29;

	/** Version 2 of the Row events */
	public static final int WRITE_ROWS_EVENT = 30;
	public static final int UPDATE_ROWS_EVENT = 31;
	public static final int DELETE_ROWS_EVENT = 32;

	public static final int GTID_LOG_EVENT = 33;
	public static final int ANONYMOUS_GTID_LOG_EVENT = 34;

	public static final int PREVIOUS_GTIDS_LOG_EVENT = 35;

	/** end marker */
	public static final int ENUM_END_EVENT = 36;

	/**
	 * 1 byte length, 1 byte format Length is total length in bytes, including 2
	 * byte header Length values 0 and 1 are currently invalid and reserved.
	 */
	public static final int EXTRA_ROW_INFO_LEN_OFFSET = 0;
	public static final int EXTRA_ROW_INFO_FORMAT_OFFSET = 1;
	public static final int EXTRA_ROW_INFO_HDR_BYTES = 2;
	public static final int EXTRA_ROW_INFO_MAX_PAYLOAD = (255 - EXTRA_ROW_INFO_HDR_BYTES);

	// Events are without checksum though its generator
	public static final int BINLOG_CHECKSUM_ALG_OFF = 0;
	// is checksum-capable New Master (NM).
	// CRC32 of zlib algorithm.
	public static final int BINLOG_CHECKSUM_ALG_CRC32 = 1;
	// the cut line: valid alg range is [1, 0x7f].
	public static final int BINLOG_CHECKSUM_ALG_ENUM_END = 2;
	// special value to tag undetermined yet checksum
	public static final int BINLOG_CHECKSUM_ALG_UNDEF = 255;
	// or events from checksum-unaware servers

	public static final int CHECKSUM_CRC32_SIGNATURE_LEN = 4;
	public static final int BINLOG_CHECKSUM_ALG_DESC_LEN = 1;
	/**
	 * defined statically while there is just one alg implemented
	 */
	public static final int BINLOG_CHECKSUM_LEN = CHECKSUM_CRC32_SIGNATURE_LEN;

	/**
	 * For an event, 'e', carrying a type code, that a slave, 's', does not
	 * recognize, 's' will check 'e' for LOG_EVENT_IGNORABLE_F, and if the flag
	 * is set, then 'e' is ignored. Otherwise, 's' acknowledges that it has
	 * found an unknown event in the relay log.
	 */
	public static final int LOG_EVENT_IGNORABLE_F = 0x80;

	/** enum_field_types */
	public static final int MYSQL_TYPE_DECIMAL = 0;
	public static final int MYSQL_TYPE_TINY = 1;
	public static final int MYSQL_TYPE_SHORT = 2;
	public static final int MYSQL_TYPE_LONG = 3;
	public static final int MYSQL_TYPE_FLOAT = 4;
	public static final int MYSQL_TYPE_DOUBLE = 5;
	public static final int MYSQL_TYPE_NULL = 6;
	public static final int MYSQL_TYPE_TIMESTAMP = 7;
	public static final int MYSQL_TYPE_LONGLONG = 8;
	public static final int MYSQL_TYPE_INT24 = 9;
	public static final int MYSQL_TYPE_DATE = 10;
	public static final int MYSQL_TYPE_TIME = 11;
	public static final int MYSQL_TYPE_DATETIME = 12;
	public static final int MYSQL_TYPE_YEAR = 13;
	public static final int MYSQL_TYPE_NEWDATE = 14;
	public static final int MYSQL_TYPE_VARCHAR = 15;
	public static final int MYSQL_TYPE_BIT = 16;
	public static final int MYSQL_TYPE_TIMESTAMP2 = 17;
	public static final int MYSQL_TYPE_DATETIME2 = 18;
	public static final int MYSQL_TYPE_TIME2 = 19;
	public static final int MYSQL_TYPE_NEWDECIMAL = 246;
	public static final int MYSQL_TYPE_ENUM = 247;
	public static final int MYSQL_TYPE_SET = 248;
	public static final int MYSQL_TYPE_TINY_BLOB = 249;
	public static final int MYSQL_TYPE_MEDIUM_BLOB = 250;
	public static final int MYSQL_TYPE_LONG_BLOB = 251;
	public static final int MYSQL_TYPE_BLOB = 252;
	public static final int MYSQL_TYPE_VAR_STRING = 253;
	public static final int MYSQL_TYPE_STRING = 254;
	public static final int MYSQL_TYPE_GEOMETRY = 255;
	// add by agapple, fixed issue: https://github.com/alibaba/canal/issues/66
	// binary/varbinary类型的处理，mysql源码中并无此类型定义(binlog里将其当作String进行解析，会被进行编码导致数据可能丢失)
	public static final int MYSQL_TYPE_BINARY = 256;
	public static final int MYSQL_TYPE_VARBINARY = 257;

	public static final int ST_SERVER_VER_LEN = 50;

	/* start event post-header (for v3 and v4) */
	public static final int ST_BINLOG_VER_OFFSET = 0;
	public static final int ST_SERVER_VER_OFFSET = 2;

	public static String getTypeName(final int type) {
		switch (type) {
		case START_EVENT_V3:
			return "Start_v3";
		case STOP_EVENT:
			return "Stop";
		case QUERY_EVENT:
			return "Query";
		case ROTATE_EVENT:
			return "Rotate";
		case INTVAR_EVENT:
			return "Intvar";
		case LOAD_EVENT:
			return "Load";
		case NEW_LOAD_EVENT:
			return "New_load";
		case SLAVE_EVENT:
			return "Slave";
		case CREATE_FILE_EVENT:
			return "Create_file";
		case APPEND_BLOCK_EVENT:
			return "Append_block";
		case DELETE_FILE_EVENT:
			return "Delete_file";
		case EXEC_LOAD_EVENT:
			return "Exec_load";
		case RAND_EVENT:
			return "RAND";
		case XID_EVENT:
			return "Xid";
		case USER_VAR_EVENT:
			return "User var";
		case FORMAT_DESCRIPTION_EVENT:
			return "Format_desc";
		case TABLE_MAP_EVENT:
			return "Table_map";
		case PRE_GA_WRITE_ROWS_EVENT:
			return "Write_rows_event_old";
		case PRE_GA_UPDATE_ROWS_EVENT:
			return "Update_rows_event_old";
		case PRE_GA_DELETE_ROWS_EVENT:
			return "Delete_rows_event_old";
		case WRITE_ROWS_EVENT_V1:
			return "Write_rows_v1";
		case UPDATE_ROWS_EVENT_V1:
			return "Update_rows_v1";
		case DELETE_ROWS_EVENT_V1:
			return "Delete_rows_v1";
		case BEGIN_LOAD_QUERY_EVENT:
			return "Begin_load_query";
		case EXECUTE_LOAD_QUERY_EVENT:
			return "Execute_load_query";
		case INCIDENT_EVENT:
			return "Incident";
		case HEARTBEAT_LOG_EVENT:
			return "Heartbeat";
		case IGNORABLE_LOG_EVENT:
			return "Ignorable";
		case ROWS_QUERY_LOG_EVENT:
			return "Rows_query";
		case WRITE_ROWS_EVENT:
			return "Write_rows";
		case UPDATE_ROWS_EVENT:
			return "Update_rows";
		case DELETE_ROWS_EVENT:
			return "Delete_rows";
		case GTID_LOG_EVENT:
			return "Gtid";
		case ANONYMOUS_GTID_LOG_EVENT:
			return "Anonymous_Gtid";
		case PREVIOUS_GTIDS_LOG_EVENT:
			return "Previous_gtids";
		default:
			return "Unknown"; /* impossible */
		}
	}

	protected static final Log logger = LogFactory
			.getLog(BinlogEventHeader.class);

	/**
	 * Return event header.
	 */
	public final BinlogEventHeader getHeader() {
		return header;
	}

	/**
	 * The total size of this event, in bytes. In other words, this is the sum
	 * of the sizes of Common-Header, Post-Header, and Body.
	 */
	public final int getEventLen() {
		return header.getEventLen();
	}

	/**
	 * Server ID of the server that created the event.
	 */
	public final long getServerId() {
		return header.getServerId();
	}

	/**
	 * The position of the next event in the master binary log, in bytes from
	 * the beginning of the file. In a binlog that is not a relay log, this is
	 * just the position of the next event, in bytes from the beginning of the
	 * file. In a relay log, this is the position of the next event in the
	 * master's binlog.
	 */
	public final long getLogPos() {
		return header.getLogPos();
	}

	/**
	 * The time when the query started, in seconds since 1970.
	 */
	public final long getWhen() {
		return header.getWhen();
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
