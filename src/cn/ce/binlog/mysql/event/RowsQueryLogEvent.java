package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

public class RowsQueryLogEvent extends IgnorableLogEvent {

	private String rowsQuery;

	public RowsQueryLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header, buffer, descriptionEvent);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		final int postHeaderLen = descriptionEvent.postHeaderLen[header
				.getType() - 1];

		/*
		 * m_rows_query length is stored using only one byte, but that length is
		 * ignored and the complete query is read.
		 */
		int offset = commonHeaderLen + postHeaderLen + 1;
		int len = buffer.limit() - offset;
		rowsQuery = buffer.getFullString(offset, len, LogBuffer.ISO_8859_1);
	}

	public String getRowsQuery() {
		return rowsQuery;
	}

}
