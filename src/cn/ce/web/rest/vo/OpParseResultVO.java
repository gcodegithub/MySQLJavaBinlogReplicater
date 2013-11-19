package cn.ce.web.rest.vo;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "OpParseResultVO")
@XmlType(propOrder = { "errorMsg", "processMsg", "eventVOList", "timestamp",
		"resCode" }, name = "OpParseResultVOType")
@XmlAccessorType(value = XmlAccessType.NONE)
public class OpParseResultVO {
	private List<String> errorMsg = new ArrayList<String>();
	private List<String> processMsg = new ArrayList<String>();
	private List<OpEventVO> eventVOList = new ArrayList<OpEventVO>();
	private Long timestamp;
	private String resCode;

	public List<OpEventVO> getEventVOList() {
		return eventVOList;
	}

	public void setEventVOList(List<OpEventVO> eventVOList) {
		this.eventVOList = eventVOList;
	}

	public void addEventVOList(OpEventVO eventVO) {
		this.eventVOList.add(eventVO);
	}

	public List<String> getErrorMsg() {
		return errorMsg;
	}

	public void setErrorMsg(List<String> errorMsg) {
		this.errorMsg = errorMsg;
	}

	public void addErrorMsg(String errorMsg) {
		this.errorMsg.add(errorMsg);
	}

	public List<String> getProcessMsg() {
		return processMsg;
	}

	public void setProcessMsg(List<String> processMsg) {
		this.processMsg = processMsg;
	}

	public void addProcessMsg(String processMsg) {
		this.processMsg.add(processMsg);
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getResCode() {
		return resCode;
	}

	public void setResCode(String resCode) {
		this.resCode = resCode;
	}

}
