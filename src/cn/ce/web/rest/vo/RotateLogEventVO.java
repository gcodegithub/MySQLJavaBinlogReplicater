package cn.ce.web.rest.vo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = { "filename", "fileBeginPosition" }, name = "RotateLogEventVOType")
public class RotateLogEventVO extends EventVO {
	private String filename;
	private long fileBeginPosition;

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public long getFileBeginPosition() {
		return fileBeginPosition;
	}

	public void setFileBeginPosition(long fileBeginPosition) {
		this.fileBeginPosition = fileBeginPosition;
	}

}
