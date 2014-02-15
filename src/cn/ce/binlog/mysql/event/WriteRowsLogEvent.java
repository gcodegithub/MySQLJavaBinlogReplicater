package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class WriteRowsLogEvent extends RowsLogEvent {
	public WriteRowsLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header, buffer, descriptionEvent);
	}
}
