package cn.ce.utils.common;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesExtend extends Properties {
	/**
	 * 
	 * @param outputstream
	 * @param s
	 *            "描述信息"
	 * @param Seperator
	 *            "分隔符"
	 * @throws IOException
	 */
	public synchronized void store(OutputStream outputstream, String s,
			String Seperator) throws IOException {
		BufferedWriter bufferedwriter = new BufferedWriter(
				new OutputStreamWriter(outputstream, "8859_1"));
		if (s != null)
			writeln(bufferedwriter, (new StringBuilder()).append("#").append(s)
					.toString());
		writeln(bufferedwriter,
				(new StringBuilder()).append("#")
						.append((new Date()).toString()).toString());
		String s1;
		String s2;
		for (Enumeration enumeration = keys(); enumeration.hasMoreElements(); writeln(
				bufferedwriter,
				(new StringBuilder()).append(s1).append(Seperator).append(s2)
						.toString())) {
			s1 = (String) enumeration.nextElement();
			s2 = (String) get(s1);
			s1 = saveConvert(s1, true);
			s2 = saveConvert(s2, false);
		}

		bufferedwriter.flush();
	}

	private static void writeln(BufferedWriter bufferedwriter, String s)
			throws IOException {
		bufferedwriter.write(s);
		bufferedwriter.newLine();
	}

	private String saveConvert(String s, boolean flag) {
		int i = s.length();
		int j = i * 2;
		if (j < 0)
			j = 2147483647;
		StringBuffer stringbuffer = new StringBuffer(j);
		for (int k = 0; k < i; k++) {
			char c = s.charAt(k);
			if (c > '=' && c < '\177') {
				if (c == '\\') {
					stringbuffer.append('\\');
					stringbuffer.append('\\');
				} else {
					stringbuffer.append(c);
				}
				continue;
			}
			switch (c) {
			case 32: // ' '
				if (k == 0 || flag)
					stringbuffer.append('\\');
				stringbuffer.append(' ');
				break;

			case 9: // '\t'
				stringbuffer.append('\\');
				stringbuffer.append('t');
				break;

			case 10: // '\n'
				stringbuffer.append('\\');
				stringbuffer.append('n');
				break;

			case 13: // '\r'
				stringbuffer.append('\\');
				stringbuffer.append('r');
				break;

			case 12: // '\f'
				stringbuffer.append('\\');
				stringbuffer.append('f');
				break;

			case 33: // '!'
			case 35: // '#'
			case 58: // ':'
			case 61: // '='
				stringbuffer.append('\\');
				stringbuffer.append(c);
				break;

			default:
				if (c < ' ' || c > '~') {
					stringbuffer.append('\\');
					stringbuffer.append('u');
					stringbuffer.append(toHex(c >> 12 & 15));
					stringbuffer.append(toHex(c >> 8 & 15));
					stringbuffer.append(toHex(c >> 4 & 15));
					stringbuffer.append(toHex(c & 15));
				} else {
					stringbuffer.append(c);
				}
				break;
			}
		}

		return stringbuffer.toString();
	}

	private static char toHex(int i) {
		return hexDigit[i & 15];
	}

	private static final char hexDigit[] = { '0', '1', '2', '3', '4', '5', '6',
			'7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

}
