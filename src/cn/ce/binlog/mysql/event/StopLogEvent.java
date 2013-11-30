package cn.ce.binlog.mysql.event;

import cn.ce.binlog.session.LogBuffer;
import cn.ce.web.rest.vo.EventVO;
import cn.ce.web.rest.vo.StopLogEventVO;

public final class StopLogEvent extends BinlogEvent {
	public StopLogEvent(BinlogEventHeader header, LogBuffer buffer,
			FormatDescriptionLogEvent description_event) {
		super(header);
	}

	public EventVO genEventVo() {
		StopLogEventVO vo = new StopLogEventVO();
		vo.setLogPos(header.getLogPos());
		vo.setBinfile(header.getBinlogfilename());
		vo.setMysqlServerId(header.getServerId());
		vo.setWhen(header.getWhen());
		vo.setEventTypeString(this.getTypeName(this.getHeader().getType()));
		return vo;
	}
}
