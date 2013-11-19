package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public class AppendBlockLogEvent extends BinlogEvent {
	private final LogBuffer blockBuf;
	private final int blockLen;

	private final long fileId;

	/* AB = "Append Block" */
	public static final int AB_FILE_ID_OFFSET = 0;

	public AppendBlockLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		final int postHeaderLen = descriptionEvent.postHeaderLen[header
				.getType() - 1];
		final int totalHeaderLen = commonHeaderLen + postHeaderLen;

		buffer.position(commonHeaderLen + AB_FILE_ID_OFFSET);
		fileId = buffer.getUint32();

		buffer.position(postHeaderLen);
		blockLen = buffer.limit() - totalHeaderLen;
		blockBuf = buffer.duplicate(blockLen);
	}

	public final long getFileId() {
		return fileId;
	}

	public final LogBuffer getBuffer() {
		return blockBuf;
	}

	public final byte[] getData() {
		return blockBuf.getData();
	}
}
