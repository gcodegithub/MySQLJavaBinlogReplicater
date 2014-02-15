package cn.ce.binlog.mysql.parse;

import java.io.IOException;

public class SocketUnexpectedEndException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7899614148065608087L;

	public SocketUnexpectedEndException(String string) {
		super(string);
	}

}
