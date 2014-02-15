package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;
import cn.ce.binlog.vo.EventVO;
import cn.ce.binlog.vo.XidLogEventVO;

public final class XidLogEvent extends BinlogEvent {
	private final long xid;

	public EventVO genEventVo() {
		XidLogEventVO vo = new XidLogEventVO();
		vo.setLogPos(header.getLogPos());
		vo.setBinfile(header.getBinlogfilename());
		vo.setMysqlServerId(header.getServerId());
		vo.setWhen(header.getWhen());
		vo.setEventTypeString(this.getTypeName(this.getHeader().getType()));
		vo.setXid(this.xid);
		return vo;
	}

	public XidLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent descriptionEvent) {
		super(header);

		/* The Post-Header is empty. The Variable Data part begins immediately. */
		buffer.position(descriptionEvent.commonHeaderLen
				+ descriptionEvent.postHeaderLen[XID_EVENT - 1]);
		xid = buffer.getLong64(); // !uint8korr
	}

	public final long getXid() {
		return xid;
	}

	public void parseXidEvent() {
		return;
	}
}
