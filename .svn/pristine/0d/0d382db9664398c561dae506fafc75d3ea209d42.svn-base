package cn.ce.binlog.mysql.util;


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TestReadWriteUtilTest {

	public static void getFixStringTest() {
		String mytest = "GoodTest123";
		byte[] byteArray = mytest.getBytes();
		String res = ReadWriteUtil.getFixString(byteArray, 0, byteArray.length,
				"UTF-8");
		System.out.println(res);
	}

	public static void mergeBytesTest() {
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

	public static void main(String[] args) {
		TestReadWriteUtilTest.getFixStringTest();
	}
}
