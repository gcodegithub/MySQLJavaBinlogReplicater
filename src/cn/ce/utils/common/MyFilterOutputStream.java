package cn.ce.utils.common;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class MyFilterOutputStream extends FilterOutputStream {
	private boolean isHasContent = false;
	private List<Byte> errors = new ArrayList<Byte>();

	public MyFilterOutputStream(OutputStream out) {
		super(out);
	}

	public boolean isHasContent() {
		return isHasContent;
	}

	public String getErrorString() {
		if (errors == null || errors.size() == 0) {
			return "";
		}
		byte[] bytes = new byte[errors.size()];
		for (int i = 0; i < errors.size(); i++) {
			bytes[i] = errors.get(i);
		}
		String errorString = new String(bytes);
		return errorString;
	}

	public void write(byte b[], int off, int len) throws IOException {
		if (len > 0) {
			this.isHasContent = true;
		}
		if ((off | len | (b.length - (len + off)) | (off + len)) < 0)
			throw new IndexOutOfBoundsException();
		for (int i = 0; i < len; i++) {
			byte oneByte = b[off + i];
			write(oneByte);
			errors.add(new Byte(oneByte));
		}
	}
}
