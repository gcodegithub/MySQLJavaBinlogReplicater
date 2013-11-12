package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public class HeartbeatLogEvent extends BinlogEvent {

	public static final int FN_REFLEN = 512; /* Max length of full path-name */
	private int identLen;
	private String logIdent;

	public HeartbeatLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		identLen = buffer.limit() - commonHeaderLen;
		if (identLen > FN_REFLEN - 1) {
			identLen = FN_REFLEN - 1;
		}

		logIdent = buffer.getFullString(commonHeaderLen, identLen,
				LogBuffer.ISO_8859_1);
	}

	public int getIdentLen() {
		return identLen;
	}

	public String getLogIdent() {
		return logIdent;
	}

}
