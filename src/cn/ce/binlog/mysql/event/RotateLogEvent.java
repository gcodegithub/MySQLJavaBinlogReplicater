package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;
import cn.ce.binlog.vo.EventVO;
import cn.ce.binlog.vo.RotateLogEventVO;

public final class RotateLogEvent extends BinlogEvent {

	private final String filename;
	private final long position;

	/* Rotate event post-header */
	public static final int R_POS_OFFSET = 0;
	public static final int R_IDENT_OFFSET = 8;

	/* Max length of full path-name */
	public static final int FN_REFLEN = 512;

	// Rotate header with all empty fields.
	public static final BinlogEventHeader ROTATE_HEADER = new BinlogEventHeader(
			ROTATE_EVENT);

	public EventVO genEventVo() {
		RotateLogEventVO vo = new RotateLogEventVO();
		vo.setLogPos(header.getLogPos());
		vo.setBinfile(header.getBinlogfilename());
		vo.setMysqlServerId(header.getServerId());
		vo.setWhen(header.getWhen());
		vo.setEventTypeString(this.getTypeName(this.getHeader().getType()));
		vo.setFileBeginPosition(this.position);
		vo.setFilename(this.filename);
		return vo;
	}

	public RotateLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int headerSize = descriptionEvent.commonHeaderLen;
		final int postHeaderLen = descriptionEvent.postHeaderLen[ROTATE_EVENT - 1];
		buffer.position(headerSize + R_POS_OFFSET);
		position = (postHeaderLen != 0) ? buffer.getLong64() : 4; // !uint8korr(buf
																	// +
																	// R_POS_OFFSET)

		final int filenameOffset = headerSize + postHeaderLen;
		int filenameLen = buffer.limit() - filenameOffset;
		if (filenameLen > FN_REFLEN - 1)
			filenameLen = FN_REFLEN - 1;
		buffer.position(filenameOffset);
		filename = buffer.getFixString(filenameLen);
	}

	/**
	 * Creates a new <code>Rotate_log_event</code> without log information. This
	 * is used to generate missing log rotation events.
	 */
	public RotateLogEvent(String filename) {
		super(ROTATE_HEADER);

		this.filename = filename;
		this.position = 4;
	}

	/**
	 * Creates a new <code>Rotate_log_event</code> without log information.
	 */
	public RotateLogEvent(String filename, final long position) {
		super(ROTATE_HEADER);

		this.filename = filename;
		this.position = position;
	}

	public final String getFilename() {
		return filename;
	}

	public final long getPosition() {
		return position;
	}
}
