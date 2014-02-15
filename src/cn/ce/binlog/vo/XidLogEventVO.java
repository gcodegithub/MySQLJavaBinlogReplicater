package cn.ce.binlog.vo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "xid" }, name = "XidLogEventVOType")
public class XidLogEventVO extends EventVO {
	private long xid;

	public long getXid() {
		return xid;
	}

	public void setXid(long xid) {
		this.xid = xid;
	}

}
