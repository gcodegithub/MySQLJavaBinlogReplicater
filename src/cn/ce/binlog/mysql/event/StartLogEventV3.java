package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public class StartLogEventV3 extends BinlogEvent {
	/**
	 * We could have used SERVER_VERSION_LENGTH, but this introduces an obscure
	 * dependency - if somebody decided to change SERVER_VERSION_LENGTH this
	 * would break the replication protocol
	 */
	public static final int ST_SERVER_VER_LEN = 50;

	/* start event post-header (for v3 and v4) */
	public static final int ST_BINLOG_VER_OFFSET = 0;
	public static final int ST_SERVER_VER_OFFSET = 2;

	protected int binlogVersion;
	protected String serverVersion;

	public StartLogEventV3(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);
		buffer.position(descriptionEvent.commonHeaderLen);
		binlogVersion = buffer.getUint16(); // ST_BINLOG_VER_OFFSET
		serverVersion = buffer.getFixString(ST_SERVER_VER_LEN); // ST_SERVER_VER_OFFSET
	}

	public StartLogEventV3() {
		super(new BinlogEventHeader(START_EVENT_V3));
	}

	public final String getServerVersion() {
		return serverVersion;
	}

	public final int getBinlogVersion() {
		return binlogVersion;
	}
}
