package cn.ce.binlog.mysql.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.pack.IPacket;
import cn.ce.binlog.mysql.parse.SocketUnexpectedEndException;

public class ReadWriteUtil {
	public static final long NULL_LENGTH = -1;

	public static void main(String[] args) {
		byte[] a = new byte[] { (byte) 0xFF, (byte) 0x22, (byte) 0x33 };
		byte[] b = new byte[] { (byte) 0x11, (byte) 0xFF };
		byte[] c = ReadWriteUtil.mergeBytes(a, b);
		String as = ToStringBuilder.reflectionToString(a,
				ToStringStyle.MULTI_LINE_STYLE);
		String bs = ToStringBuilder.reflectionToString(b,
				ToStringStyle.MULTI_LINE_STYLE);
		String cs = ToStringBuilder.reflectionToString(c,
				ToStringStyle.MULTI_LINE_STYLE);
		System.out.println(as);
		System.out.println(bs);
		System.out.println(cs);
	}

	public static String getFixString(byte[] byteArray, final int pos,
			final int len, String charsetName) {

		final int from = pos;
		final int end = from + len;
		byte[] buf = byteArray;
		int found = from;
		for (; (found < end) && buf[found] != '\0'; found++)
			/* empty loop */;
		try {
			String string = new String(buf, from, found - from, charsetName);
			return string;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unsupported encoding: "
					+ charsetName, e);
		}
	}

	public static byte[] mergeBytes(byte[]... toMerge) {
		byte[] res = null;
		for (byte[] needAdd : toMerge) {
			if (res != null) {
				byte[] temp = new byte[res.length + needAdd.length];
				System.arraycopy(res, 0, temp, 0, res.length);
				System.arraycopy(needAdd, 0, temp, res.length, needAdd.length);
				res = temp;
			} else {
				res = needAdd;
			}
			String ress = ToStringBuilder.reflectionToString(res,
					ToStringStyle.MULTI_LINE_STYLE);
		}
		return res;
	}

	public static void write(SocketChannel ch, ByteBuffer[] srcs)
			throws IOException {
		long total = 0;
		for (ByteBuffer buffer : srcs) {
			total += buffer.remaining();
		}
		ch.write(srcs);
	}

	public static byte[] readNullTerminatedBytes(byte[] data, int index) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (int i = index; i < data.length; i++) {
			byte item = data[i];
			if (item == IPacket.NULL_TERMINATED_STRING_DELIMITER) {
				break;
			}
			out.write(item);
		}
		return out.toByteArray();
	}

	public static void writeNullTerminatedString(String str,
			ByteArrayOutputStream out) throws IOException {
		out.write(str.getBytes());
		out.write(IPacket.NULL_TERMINATED_STRING_DELIMITER);
	}

	public static void writeNullTerminated(byte[] data,
			ByteArrayOutputStream out) throws IOException {
		out.write(data);
		out.write(IPacket.NULL_TERMINATED_STRING_DELIMITER);
	}

	public static byte[] readFixedLengthBytes(byte[] data, int index, int length) {
		byte[] bytes = new byte[length];
		System.arraycopy(data, index, bytes, 0, length);
		return bytes;
	}

	public static int getUint8(byte[] bytes, int position) {
		return 0xff & bytes[position];
	}

	/**
	 * Read 4 bytes in Little-endian byte order.
	 * 
	 * @param data
	 *            , the original byte array
	 * @param index
	 *            , start to read from.
	 * @return
	 */
	// 4字節
	public static long readUnsignedIntLittleEndian(byte[] data, int index) {
		long result = (long) (data[index] & 0xFF)
				| (long) ((data[index + 1] & 0xFF) << 8)
				| (long) ((data[index + 2] & 0xFF) << 16)
				| (long) ((data[index + 3] & 0xFF) << 24);
		return result;
	}

	// 8字節
	public static long readUnsignedLongLittleEndian(byte[] data, int index) {
		long accumulation = 0;
		int position = index;
		for (int shiftBy = 0; shiftBy < 64; shiftBy += 8) {
			accumulation |= (long) ((data[position++] & 0xff) << shiftBy);
		}
		return accumulation;
	}

	// 2字節
	public static int readUnsignedShortLittleEndian(byte[] data, int index) {
		int result = (data[index] & 0xFF) | ((data[index + 1] & 0xFF) << 8);
		return result;
	}

	// 3字节
	public static int readUnsignedMediumLittleEndian(byte[] data, int index) {
		int result = (data[index] & 0xFF) | ((data[index + 1] & 0xFF) << 8)
				| ((data[index + 2] & 0xFF) << 16);
		return result;
	}

	public static long readLengthCodedBinary(byte[] data, int index)
			throws IOException {
		int firstByte = data[index] & 0xFF;
		switch (firstByte) {
		case 251:
			return NULL_LENGTH;
		case 252:
			return readUnsignedShortLittleEndian(data, index + 1);
		case 253:
			return readUnsignedMediumLittleEndian(data, index + 1);
		case 254:
			return readUnsignedLongLittleEndian(data, index + 1);
		default:
			return firstByte;
		}
	}

	public static byte[] readBinaryCodedLengthBytes(byte[] data, int index)
			throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(data[index]);

		byte[] buffer = null;
		int value = data[index] & 0xFF;
		if (value == 251) {
			buffer = new byte[0];
		}
		if (value == 252) {
			buffer = new byte[2];
		}
		if (value == 253) {
			buffer = new byte[3];
		}
		if (value == 254) {
			buffer = new byte[8];
		}
		if (buffer != null) {
			System.arraycopy(data, index + 1, buffer, 0, buffer.length);
			out.write(buffer);
		}

		return out.toByteArray();
	}

	public static void writeUnsignedIntLittleEndian(long data,
			ByteArrayOutputStream out) {
		out.write((byte) (data & 0xFF));
		out.write((byte) (data >>> 8));
		out.write((byte) (data >>> 16));
		out.write((byte) (data >>> 24));
	}

	public static void writeUnsignedShortLittleEndian(int data,
			ByteArrayOutputStream out) {
		out.write((byte) (data & 0xFF));
		out.write((byte) ((data >>> 8) & 0xFF));
	}

	public static void writeUnsignedMediumLittleEndian(int data,
			ByteArrayOutputStream out) {
		out.write((byte) (data & 0xFF));
		out.write((byte) ((data >>> 8) & 0xFF));
		out.write((byte) ((data >>> 16) & 0xFF));
	}

	public static void writeBinaryCodedLengthBytes(byte[] data,
			ByteArrayOutputStream out) throws IOException {
		// 1. write length byte/bytes
		if (data.length < 252) {
			out.write((byte) data.length);
		} else if (data.length < (1 << 16L)) {
			out.write((byte) 252);
			writeUnsignedShortLittleEndian(data.length, out);
		} else if (data.length < (1 << 24L)) {
			out.write((byte) 253);
			writeUnsignedMediumLittleEndian(data.length, out);
		} else {
			out.write((byte) 254);
			writeUnsignedIntLittleEndian(data.length, out);
		}
		// 2. write real data followed length byte/bytes
		out.write(data);
	}

	public static void writeFixedLengthBytes(byte[] data, int index,
			int length, ByteArrayOutputStream out) {
		for (int i = index; i < index + length; i++) {
			out.write(data[i]);
		}
	}

	public static void writeFixedLengthBytesFromStart(byte[] data, int length,
			ByteArrayOutputStream out) {
		writeFixedLengthBytes(data, 0, length, out);
	}

	public static ByteBuffer readBytesAsBuffer(SocketChannel ch, int len)
			throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(len);
		while (buffer.hasRemaining()) {
			int readNum = ch.read(buffer);
			if (readNum == -1) {
				throw new SocketUnexpectedEndException("SocketChannel Unexpected End");
			}
		}
		return buffer;
	}

	public static byte[] readBytes(SocketChannel ch, int len)
			throws IOException {
		return readBytesAsBuffer(ch, len).array();
	}
}
