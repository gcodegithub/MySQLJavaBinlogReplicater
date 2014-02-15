package cn.ce.binlog.mysql.util;

import java.math.BigInteger;

public class MySQLColumnUtil {

	public static final int TINYINT_MAX_VALUE = 256;
	public static final int SMALLINT_MAX_VALUE = 65536;
	public static final int MEDIUMINT_MAX_VALUE = 16777216;
	public static final long INTEGER_MAX_VALUE = 4294967296L;
	public static final BigInteger BIGINT_MAX_VALUE = new BigInteger(
			"18446744073709551616");

	public static boolean isText(String columnType) {
		return "LONGTEXT".equalsIgnoreCase(columnType)
				|| "MEDIUMTEXT".equalsIgnoreCase(columnType)
				|| "TEXT".equalsIgnoreCase(columnType)
				|| "TINYTEXT".equalsIgnoreCase(columnType);
	}
}
