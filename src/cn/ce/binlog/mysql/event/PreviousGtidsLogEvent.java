package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

/**
 * 

 */
public class PreviousGtidsLogEvent extends BinlogEvent {

	public PreviousGtidsLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);
		// do nothing , just for mysql gtid search function
	}
}
