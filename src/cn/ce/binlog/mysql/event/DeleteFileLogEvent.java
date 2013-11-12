package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class DeleteFileLogEvent extends BinlogEvent {
	private final long fileId;

	/* DF = "Delete File" */
	public static final int DF_FILE_ID_OFFSET = 0;

	public DeleteFileLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		buffer.position(commonHeaderLen + DF_FILE_ID_OFFSET);
		fileId = buffer.getUint32(); // DF_FILE_ID_OFFSET
	}

	public final long getFileId() {
		return fileId;
	}
}
