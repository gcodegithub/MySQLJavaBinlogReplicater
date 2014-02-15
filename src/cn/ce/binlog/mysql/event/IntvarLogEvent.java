package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class IntvarLogEvent extends BinlogEvent {

	private final long value;
	private final int type;

	/* Intvar event data */
	public static final int I_TYPE_OFFSET = 0;
	public static final int I_VAL_OFFSET = 1;

	// enum Int_event_type
	public static final int INVALID_INT_EVENT = 0;
	public static final int LAST_INSERT_ID_EVENT = 1;
	public static final int INSERT_ID_EVENT = 2;

	public IntvarLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		/* The Post-Header is empty. The Varible Data part begins immediately. */
		buffer.position(descriptionEvent.commonHeaderLen
				+ descriptionEvent.postHeaderLen[INTVAR_EVENT - 1]
				+ I_TYPE_OFFSET);
		type = buffer.getInt8(); // I_TYPE_OFFSET
		value = buffer.getLong64(); // !uint8korr(buf + I_VAL_OFFSET);
	}

	public final int getType() {
		return type;
	}

	public final long getValue() {
		return value;
	}

	public final String getQuery() {
		return "SET INSERT_ID = " + value;
	}
}
