package cn.ce.binlog.vo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.mongodb.DBObject;

@XmlRootElement(name = "OpParseResultVO")
@XmlType(propOrder = { "eventVOList", "timestamp", "inc", "resCode" }, name = "OpParseResultVOType")
@XmlAccessorType(value = XmlAccessType.NONE)
public class OpParseResultVO {
	private List<DBObject> eventVOList = new ArrayList<DBObject>();
	private Integer timestamp;
	private String inc;
	private String resCode;

	public List<DBObject> getEventVOList() {
		return eventVOList;
	}

	public void setEventVOList(List<DBObject> eventVOList) {
		this.eventVOList = eventVOList;
	}

	public void addEventVOList(DBObject eventVO) {
		this.eventVOList.add(eventVO);
	}

	public String getInc() {
		return inc;
	}

	public void setInc(String inc) {
		this.inc = inc;
	}

	public Integer getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Integer timestamp) {
		this.timestamp = timestamp;
	}

	public String getResCode() {
		return resCode;
	}

	public void setResCode(String resCode) {
		this.resCode = resCode;
	}

}
