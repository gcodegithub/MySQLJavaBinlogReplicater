package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class ExecuteLoadLogEvent extends BinlogEvent {
	private final long fileId;

	/* EL = "Execute Load" */
	public static final int EL_FILE_ID_OFFSET = 0;

	public ExecuteLoadLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		buffer.position(commonHeaderLen + EL_FILE_ID_OFFSET);
		fileId = buffer.getUint32(); // EL_FILE_ID_OFFSET
	}

	public final long getFileId() {
		return fileId;
	}
}
