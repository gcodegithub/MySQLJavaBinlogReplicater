package cn.ce.utils.common;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicLong;

public class RAFOutputStream extends OutputStream {
	private RandomAccessFile raf = null;
	private AtomicLong length = new AtomicLong(0);

	public RAFOutputStream(RandomAccessFile raf) {
		this.raf = raf;
	}

	@Override
	public void write(int b) throws IOException {
		long position = raf.getFilePointer();
		raf.write(b);
		length.getAndIncrement();
	}

	@Override
	public void close() throws IOException {
		if (raf != null) {
			raf.getChannel().close();
		}
	}

	public Long getLength() {
		return length.longValue();
	}

}
