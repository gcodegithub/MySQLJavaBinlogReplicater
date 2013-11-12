package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class DeleteRowsLogEvent extends RowsLogEvent {
	public DeleteRowsLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header, buffer, descriptionEvent);
	}
}
