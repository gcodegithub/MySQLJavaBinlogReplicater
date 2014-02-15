package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;

/**

 */
public class GtidLogEvent extends BinlogEvent {

	// / Length of the commit_flag in event encoding
	public static final int ENCODED_FLAG_LENGTH = 1;
	// / Length of SID in event encoding
	public static final int ENCODED_SID_LENGTH = 16;

	private boolean commitFlag;

	public GtidLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		final int commonHeaderLen = descriptionEvent.commonHeaderLen;
		// final int postHeaderLen = descriptionEvent.postHeaderLen[header.type
		// - 1];

		buffer.position(commonHeaderLen);
		commitFlag = (buffer.getUint8() != 0); // ENCODED_FLAG_LENGTH

		// ignore gtid info read
		// sid.copy_from((uchar *)ptr_buffer);
		// ptr_buffer+= ENCODED_SID_LENGTH;
		//
		// // SIDNO is only generated if needed, in get_sidno().
		// spec.gtid.sidno= -1;
		//
		// spec.gtid.gno= uint8korr(ptr_buffer);
		// ptr_buffer+= ENCODED_GNO_LENGTH;
	}

	public boolean isCommitFlag() {
		return commitFlag;
	}

}
