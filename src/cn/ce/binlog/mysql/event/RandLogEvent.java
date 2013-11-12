package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class RandLogEvent extends BinlogEvent {
	/**
	 * Fixed data part: Empty
	 * 
	 * <p>
	 * Variable data part:
	 * 
	 * <ul>
	 * <li>8 bytes. The value for the first seed.</li>
	 * <li>8 bytes. The value for the second seed.</li>
	 * </ul>
	 * 
	 * Source : http://forge.mysql.com/wiki/MySQL_Internals_Binary_Log
	 */
	private final long seed1;
	private final long seed2;

	/* Rand event data */
	public static final int RAND_SEED1_OFFSET = 0;
	public static final int RAND_SEED2_OFFSET = 8;

	public RandLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		/* The Post-Header is empty. The Variable Data part begins immediately. */
		buffer.position(descriptionEvent.commonHeaderLen
				+ descriptionEvent.postHeaderLen[RAND_EVENT - 1]
				+ RAND_SEED1_OFFSET);
		seed1 = buffer.getLong64(); // !uint8korr(buf+RAND_SEED1_OFFSET);
		seed2 = buffer.getLong64(); // !uint8korr(buf+RAND_SEED2_OFFSET);
	}

	public final String getQuery() {
		return "SET SESSION rand_seed1 = " + seed1 + " , rand_seed2 = " + seed2;
	}
}
