package cn.ce.binlog.mysql.pack;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public abstract class CommandPacket implements IPacket {

	private byte command;

	public void setCommand(byte command) {
		this.command = command;
	}

	public byte getCommand() {
		return command;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
