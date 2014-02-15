package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public final class UpdateRowsLogEvent extends RowsLogEvent {
	public UpdateRowsLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header, buffer, descriptionEvent);
	}
}
