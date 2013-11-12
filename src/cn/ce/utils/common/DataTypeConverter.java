package cn.ce.utils.common;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataTypeConverter {
	/**
	 * 从object中获取String值
	 * @param object
	 * @return String 
	 */
	public static String getStringFromObject(Object object) {
		return getStringFromObject(object, "");
	}

	public static String getStringFromObject(Object object,
			String defaultReturnValue) {
		String rtn = null;
		if (object == null) {
			rtn = defaultReturnValue;
		} else if (object instanceof String) {
			rtn = object.toString();
		} else {
			try {
				rtn = String.valueOf(object);
			} catch (Exception e) {
				rtn = defaultReturnValue;
			}
		}
		return rtn;
	}

	/**
	 * 从object中获取int值
	 * 
	 * @param object
	 * @return
	 */
	public static int getInt32FromObject(Object object) {
		return getInt32FromObject(object, 0);
	}

	public static int getInt32FromObject(Object object, int defaultReturnValue) {
		int rtn = 0;
		if (object == null) {
			rtn = defaultReturnValue;
		} else if (object instanceof Integer) {
			rtn = ((Integer) object).intValue();
		} else if (object instanceof Double) {
			rtn = ((Double) object).intValue();
		} else if (object instanceof Long) {
			rtn = ((Long) object).intValue();
		} else {
			try {
				rtn = Integer.parseInt(object.toString());
			} catch (Exception e) {
				rtn = defaultReturnValue;
			}
		}
		return rtn;
	}

	/**
	 * 从object中获取short类型值
	 * 
	 * @param object
	 * @return short
	 */
	public static short getInt16FromObject(Object object) {
		return getInt16FromObject(object, (short) 0);
	}

	public static short getInt16FromObject(Object object,
			short defaultReturnValue) {
		short rtn = 0;
		if (object == null) {
			rtn = defaultReturnValue;
		} else if (object instanceof Short) {
			rtn = ((Short) object).shortValue();
		} else if (object instanceof Double) {
			rtn = ((Double) object).shortValue();
		} else if (object instanceof Long) {
			rtn = ((Long) object).shortValue();
		} else {
			try {
				rtn = Short.parseShort(object.toString());
			} catch (Exception e) {
				rtn = defaultReturnValue;
			}
		}
		return rtn;
	}

	/**
	 * 从object中获取long类型值
	 * 
	 * @param object
	 * @return long
	 */
	public static long getInt64FromObject(Object object) {
		return getInt64FromObject(object, (long) 0);
	}

	public static long getInt64FromObject(Object object, long defaultReturnVlaue) {
		long rtn = 0;
		if (object == null) {
			rtn = defaultReturnVlaue;
		} else if (object instanceof Long) {
			rtn = ((Long) object).longValue();
		} else if (object instanceof Double) {
			rtn = ((Double) object).longValue();
		} else {
			try {
				rtn = Long.parseLong(object.toString());
			} catch (Exception e) {
				rtn = defaultReturnVlaue;
			}
		}
		return rtn;
	}

	/**
	 * 从object 中取出 BigDecimal值
	 * 
	 * @param object
	 * @return
	 */
	public static BigDecimal getDecimalFromObject(Object object) {
		return getDecimalFromObject(object, BigDecimal.ZERO);
	}

	public static BigDecimal getDecimalFromObject(Object object,
			BigDecimal defaultReturnValue) {
		BigDecimal rtn = BigDecimal.ZERO;
		if (object == null) {
			rtn = defaultReturnValue;
		} else {
			try {
				rtn = new BigDecimal(object.toString());
			} catch (Exception e) {
				rtn = defaultReturnValue;
			}
		}
		return rtn;
	}

	/**
	 * 从object中取回boolean值
	 * 
	 * @param object
	 * @return true or false
	 */
	public static Boolean getBooleanFromObject(Object object) {
		return getBooleanFromObject(object, Boolean.FALSE);
	}

	public static Boolean getBooleanFromObject(Object object,
			boolean defaultReturnValue) {
		boolean rtn = false;
		if (object == null) {
			rtn = defaultReturnValue;
		} else if (object instanceof Boolean) {
			rtn = ((Boolean) object).booleanValue();
		} else {
			try {
				rtn = Boolean.parseBoolean(object.toString());
			} catch (Exception e) {
				rtn = defaultReturnValue;
			}
		}
		return rtn;
	}

	private static Date getDateFromString(String pattern, String dateStr) {
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		try {
			if (null == dateStr)
				return null;
			return sdf.parse(dateStr);
		} catch (ParseException e) {
			try {
				return sdf.parse("1900-01-01 00:00:00");
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * 从object 中获取Date值
	 * 
	 * @param object
	 * @param pattern
	 *            时间的格式，如"yyyy-MM-dd"
	 * @return
	 * @throws ParseException
	 */
	public static Date getDateFromObject(Object object, String pattern) {
		return getDateFromObject(object, "1900-01-01", pattern);
	}

	public static Date getDateFromObject(Object object,
			String defaultReturnValue, String pattern) {
		Date rtn = null;
		if (object == null) {
			rtn = getDateFromString(pattern, defaultReturnValue);
		} else if (object instanceof Date) {
			rtn = (Date) object;
		} else {
			try {
				rtn = getDateFromString(pattern, object.toString());
			} catch (Exception e) {
				rtn = getDateFromString(pattern, defaultReturnValue);
			}
		}
		return rtn;
	}

}
