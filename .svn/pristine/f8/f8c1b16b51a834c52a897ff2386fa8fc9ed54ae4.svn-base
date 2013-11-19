package cn.ce.utils.common;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Text {

	private Text() {
	}

	/**
	 * 取得指定长度的随机数
	 * 
	 * @param len
	 *            int 指定长度
	 * @return String 指定长度的随机数
	 */
	public static String randomString(int len) {
		int base = (int) Math.pow(10, len) - 1;
		return String.valueOf(new java.security.SecureRandom().nextInt(base));
	}

	/**
	 * 判断字符是否为空
	 * 
	 * @param str
	 *            String 字符串
	 * @return boolean 判断字符串是否为空
	 */
	public static boolean isEmpty(String str) {
		return (str == null || str.trim().length() == 0);
	}

	/**
	 * 判断是否为数字字符串
	 * 
	 * @param str
	 *            String 字符串
	 * @return boolean 判断是否为字符字符串
	 */
	public static boolean isNumber(String strNum) {
		if (strNum.matches("^[0-9]*[1-9][0-9]*$")) {
			return true;
		}
		return false;
	}

	/**
	 * 取得User-Agent
	 * 
	 * @param userAgent
	 *            String 用户User-Agent
	 * @return String User-Agent
	 */
	public static String parseUA(String userAgent) {
		int index = userAgent.indexOf(' ');
		if (index == -1) {
			index = userAgent.indexOf('/');
		}
		if (index == -1) {
			index = (userAgent.length() >= 30 ? 30 : userAgent.length());
		}
		return userAgent.substring(0, index);
	}

	/**
	 * 获取指定长度的字符串
	 * 
	 * @param str
	 *            String 指定字符串
	 * @param len
	 *            int 指定长度
	 * @return String 指定长度的字符串
	 */
	public static String getSubstring(String str, int len) {
		if (str != null && str.trim().length() > 0) {
			char[] cs = str.toCharArray();
			int count = 0;
			int last = cs.length;
			for (int i = 0; i < cs.length; i++) {
				if (cs[i] > 255) {
					count += 2;
				} else {
					count++;
				}
				if (count > len) {
					last = i + 1;
					break;
				}
			}
			if (count < len) {
				return str;
			}
			len -= 2;
			for (int i = last - 1; i >= 0; i--) {
				if (cs[i] > 255) {
					count -= 2;
				} else {
					count--;
				}
				if (count <= len) {
					return str.substring(0, i) + "…";
				}
			}
			return "…";
		}
		return null;
	}

	/**
	 * 对字符排序
	 * 
	 * @param str
	 *            Object[] 排序的字符
	 * @return Object[] 排序数组
	 */
	public static Object[] sort(Object[] str) {
		if (str != null && str.length > 0) {
			int[] fm = new int[str.length];
			Object[] file = new Object[str.length];
			for (int i = 0; i < str.length; i++) {
				String key = (String) str[i];
				int num = key.lastIndexOf("e");
				fm[i] = Integer.parseInt(key.substring(num + 1));
			}
			Arrays.sort(fm);
			for (int i = 0; i < fm.length; i++) {
				file[i] = "file" + String.valueOf(fm[i]);
			}
			return file;
		}
		return null;
	}

	/**
	 * 利用正则表达式判断是否含有某字符
	 * 
	 * @param str
	 *            需要判断的字符
	 * @param regEx
	 *            正则表达式
	 * @return boolean 判断是否含有某字符
	 */
	public static boolean ifRegEx(String str, String regEx) {
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		boolean rs = m.find();
		return rs;

	}

	/**
	 * 使用初始值过滤空字符串
	 * 
	 * @param str
	 *            String 字符串
	 * @param defauleValue
	 *            String 初始值
	 * @return String 过滤后的字符串
	 */
	public static String filterNull(String str, String defauleValue) {
		String s = "";
		if (str == null || str.trim().length() == 0) {
			s = defauleValue;
		} else {
			s = str.trim();
		}
		return s;
	}

	/**
	 * 判断是否为数字字符串
	 * 
	 * @param str
	 *            String 字符串
	 * @return boolean 判断是否为字符字符串
	 */
	public static boolean isNum(String str) {
		boolean ret = true;
		try {
			new Integer(str);
		} catch (NumberFormatException e) {
			ret = false;
		}

		return ret;
	}

	/**
	 * 判断是否为带小数的数字字符串
	 * 
	 * @param str
	 *            String 字符串
	 * @return boolean 判断是否为字符字符串
	 */
	public static boolean isDouble(String str) {
		boolean ret = true;
		try {
			new Double(str);
		} catch (NumberFormatException e) {
			ret = false;
		}

		return ret;
	}



	/**
	 * escape转中文
	 * 
	 * @param src
	 *            String
	 * @return String
	 */
	public static String escape(String src) {
		int i;
		char j;
		StringBuffer tmp = new StringBuffer();
		tmp.ensureCapacity(src.length() * 6);
		for (i = 0; i < src.length(); i++) {
			j = src.charAt(i);
			if (Character.isDigit(j) || Character.isLowerCase(j)
					|| Character.isUpperCase(j))
				tmp.append(j);
			else if (j < 256) {
				tmp.append("%");
				if (j < 16)
					tmp.append("0");
				tmp.append(Integer.toString(j, 16));
			} else {
				tmp.append("%u");
				tmp.append(Integer.toString(j, 16));
			}
		}
		return tmp.toString();
	}

	/**
	 * unescape转中文
	 * 
	 * @param src
	 *            String
	 * @return
	 */
	public static String unescape(String src) {
		StringBuffer tmp = new StringBuffer();
		tmp.ensureCapacity(src.length());
		int lastPos = 0, pos = 0;
		char ch;
		while (lastPos < src.length()) {
			pos = src.indexOf("%", lastPos);
			if (pos == lastPos) {
				if (src.charAt(pos + 1) == 'u') {
					ch = (char) Integer.parseInt(src
							.substring(pos + 2, pos + 6), 16);
					tmp.append(ch);
					lastPos = pos + 6;
				} else {
					ch = (char) Integer.parseInt(src
							.substring(pos + 1, pos + 3), 16);
					tmp.append(ch);
					lastPos = pos + 3;
				}
			} else {
				if (pos == -1) {
					tmp.append(src.substring(lastPos));
					lastPos = src.length();
				} else {
					tmp.append(src.substring(lastPos, pos));
					lastPos = pos;
				}
			}
		}
		return tmp.toString();
	}

	/**
	 * 判断time1和time2大小
	 * 
	 * @param time1
	 *            (yyyy-mm-dd)
	 * @param time2
	 *            (yyyy-mm-dd)
	 * @return 如果time1大于taime2，返回true，否则返回false
	 */
	public static boolean compareTwoDate(String time1, String time2)
			throws ParseException {
		int minuteInterval;
		boolean b = false;
		if (time1.equals(""))
			return false;
		if (time2.equals(""))
			return false;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date t1 = sdf.parse(time1);
		Date t2 = sdf.parse(time2);

		minuteInterval = t1.compareTo(t2);
		if (minuteInterval > 0)
			b = true;
		else
			b = false;
		return b;
	}

	/**
	 * 返回服务器当前的日期，日期格式为"yyyy-mm-dd"
	 * 
	 * @return
	 */
	public static String getCurrentDate() {
		Calendar source_cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setCalendar(source_cal);
		return sdf.format(source_cal.getTime());
	}

	/**
	 * 返回两个日期相差的天数
	 * 
	 * @param time1
	 *            数据格式为yyyy-mm-dd
	 * @param time2
	 *            数据格式为yyyy-mm-dd
	 * @return 返回两个日期相差的天数（date2－date1）,两个日期都不允许为空，否则返回0
	 * @throws ParseException
	 */
	public static long getDiffDayCount(String date1, String date2)
			throws ParseException {
		long quot = 0;
		if (date1.equals(""))
			return 0;
		if (date2.equals(""))
			return 0;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date t1 = sdf.parse(date1);
		Date t2 = sdf.parse(date2);
		quot = t2.getTime() - t1.getTime();
		return quot / 1000 / 60 / 60 / 24;
	}

	/**
	 * 返回两个日期相差的天数
	 * 
	 * @param date1
	 * @param date2
	 * @return 返回两个日期相差的天数（date2－date1）,两个日期都不允许为空，否则返回0
	 */
	public static long getDiffDayCount(Date date1, Date date2) {
		long quot = 0;
		if (date1 == null)
			return 0;
		if (date2 == null)
			return 0;
		quot = date2.getTime() - date1.getTime();
		return quot / 1000 / 60 / 60 / 24;
	}

	/**
	 * 根据周期类型和周期日期，返回周期初日期
	 * 
	 * @param bIsPeriodStart
	 *            true－取周期初日期，false－取周期末日期
	 * @param iPeriodType
	 *            周期类型（0－日，1－周，2－旬，3－月，4－季，5－半年，6－年）
	 * @param date
	 *            周期日期,格式为yyyy-MM-dd
	 * @return 周期期初/期末日期，如果周期日期为NULL或者是周期类型小于0或大于6，则返回的期初/期末日期为NULL
	 * @throws ParseException
	 */
	public static Date getPeriodDate(boolean bIsPeriodStart, int iPeriodType,
			String date) throws ParseException {
		if (date == null || date.equals("")) {
			return null;
		} else {
			SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
			return getPeriodDate(bIsPeriodStart, iPeriodType, df1.parse(date));
		}
	}

	/**
	 * 根据周期类型和周期日期，返回周期初日期
	 * 
	 * @param bIsPeriodStart
	 *            true－取周期初日期，false－取周期末日期
	 * @param iPeriodType
	 *            周期类型（0－日，1－周，2－旬，3－月，4－季，5－半年，6－年）
	 * @param date
	 *            周期日期
	 * @return 周期期初/期末日期，如果周期日期为NULL或者是周期类型小于0或大于6，则返回的期初/期末日期为NULL
	 */
	@SuppressWarnings("deprecation")
	public static Date getPeriodDate(boolean bIsPeriodStart, int iPeriodType,
			Date date) {
		if (date == null) {
			return null;
		} else if (iPeriodType < 0 || iPeriodType > 6) {
			return null;
		} else {
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			int iYear = cal.get(Calendar.YEAR);// 日期－年
			int iMonth = cal.get(Calendar.MONTH) + 1;// 日期－月份，当前月份月份从0开始，所以要加1
			int iDay = cal.get(Calendar.DAY_OF_MONTH);// 日期－天
			int iDays = getDaysByMonth(IsLeapYear(iYear), iMonth);// 日期月份的天数
			if (iPeriodType == 0) {
				/* 日 */
				return date;
			} else if (iPeriodType == 1) {
				/* 周 */
				int iDayIndex = date.getDay();// 取出此日期是周几
				if (bIsPeriodStart) {
					cal.add(Calendar.DATE, -iDayIndex);
				} else {
					cal.add(Calendar.DATE, 7 - iDayIndex - 1);
				}
			} else if (iPeriodType == 2) {
				/* 旬 */
				if (iDay <= 10) {
					if (bIsPeriodStart) {
						cal.add(Calendar.DATE, -iDay + 1);
					} else {
						cal.add(Calendar.DATE, 10 - iDay);
					}
				} else if (iDay > 10 && iDay <= 20) {
					if (bIsPeriodStart) {
						cal.add(Calendar.DATE, -9);
					} else {
						cal.add(Calendar.DATE, 20 - iDay);
					}
				} else {
					if (bIsPeriodStart) {
						cal.add(Calendar.DATE,
								-(cal.get(Calendar.DAY_OF_MONTH) - 20));
					} else {
						cal.add(Calendar.DATE, iDays - iDay);
					}
				}
			} else if (iPeriodType == 3) {
				/* 月 */
				if (bIsPeriodStart) {
					cal.add(Calendar.DATE, -iDay + 1);
				} else {
					cal.add(Calendar.DATE, -iDay + iDays);
				}
			} else if (iPeriodType == 4) {
				/* 季 */
				int iQuarter = getQuarterNum(iMonth);// 日期月份所在季度数
				if (iQuarter == 1) {
					if (bIsPeriodStart) {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 0);
						cal.set(Calendar.DAY_OF_MONTH, 1);
					} else {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 2);
						cal.set(Calendar.DAY_OF_MONTH, 31);
					}
				} else if (iQuarter == 2) {
					if (bIsPeriodStart) {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 3);
						cal.set(Calendar.DAY_OF_MONTH, 1);
					} else {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 5);
						cal.set(Calendar.DAY_OF_MONTH, 30);
					}
				} else if (iQuarter == 3) {
					if (bIsPeriodStart) {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 6);
						cal.set(Calendar.DAY_OF_MONTH, 1);
					} else {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 8);
						cal.set(Calendar.DAY_OF_MONTH, 30);
					}
				} else {
					if (bIsPeriodStart) {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 9);
						cal.set(Calendar.DAY_OF_MONTH, 1);
					} else {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 11);
						cal.set(Calendar.DAY_OF_MONTH, 31);
					}
				}

			} else if (iPeriodType == 5) {
				/* 半年 */
				if (iMonth < 7) {
					/* 上半年 */
					if (bIsPeriodStart) {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 0);
						cal.set(Calendar.DAY_OF_MONTH, 1);
					} else {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 5);
						cal.set(Calendar.DAY_OF_MONTH, 30);
					}
				} else {
					/* 下半年 */
					if (bIsPeriodStart) {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 6);
						cal.set(Calendar.DAY_OF_MONTH, 1);
					} else {
						cal.set(Calendar.YEAR, iYear);
						cal.set(Calendar.MONTH, 11);
						cal.set(Calendar.DAY_OF_MONTH, 31);
					}
				}
			} else if (iPeriodType == 6) {
				/* 年 */
				if (bIsPeriodStart) {
					cal.set(Calendar.YEAR, iYear);
					cal.set(Calendar.MONTH, 0);
					cal.set(Calendar.DAY_OF_MONTH, 1);
				} else {
					cal.set(Calendar.YEAR, iYear);
					cal.set(Calendar.MONTH, 11);
					cal.set(Calendar.DAY_OF_MONTH, 31);
				}
			} else {
				return null;
			}
			return cal.getTime();
		}
	}

	/**
	 * 根据年份判断是否是闰年
	 * 
	 * @param iYear
	 *            当前年份
	 * @return
	 */
	private static Boolean IsLeapYear(int iYear) {
		if (iYear % 400 == 0 || (iYear % 4 == 0 && iYear % 100 == 0)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 通过给出是否闰年和具体的月份返回前月份的天数
	 * 
	 * @param bIsLeapYear
	 *            是否是闰年
	 * @param iMonth
	 *            月份数
	 * @return 前月份的天数
	 */
	private static int getDaysByMonth(Boolean bIsLeapYear, int iMonth) {
		if (iMonth == 1 || iMonth == 3 || iMonth == 5 || iMonth == 7
				|| iMonth == 8 || iMonth == 10 || iMonth == 12) {
			return 31;
		} else if (iMonth == 4 || iMonth == 6 || iMonth == 9 || iMonth == 11) {
			return 30;
		} else {
			if (bIsLeapYear) {
				return 28;
			} else {
				return 29;
			}
		}
	}

	/**
	 * 通过月份返回当前月是第几季度（1，2，3，4）
	 * 
	 * @param iMonth
	 *            月份
	 * @return 季度数
	 */
	private static int getQuarterNum(int iMonth) {
		if (iMonth == 1 || iMonth == 2 || iMonth == 3) {
			return 1;
		} else if (iMonth == 4 || iMonth == 5 || iMonth == 6) {
			return 2;
		} else if (iMonth == 7 || iMonth == 8 || iMonth == 9) {
			return 3;
		} else {
			return 4;
		}
	}

	/**
	 * 通过年份和季度数返回当前季度的开始日期
	 * 
	 * @param String
	 *            sYear 年份
	 * @param int iSeason 季度数（1，2，3，4）
	 * @return 季度数
	 */
	public static String getSeasonSDate(String sYear, int iSeason) {
		if (iSeason < 0 || iSeason > 4)
			return "";
		if (iSeason == 1) {
			return sYear + "-01-01";
		} else if (iSeason == 2) {
			return sYear + "-04-01";
		} else if (iSeason == 3) {
			return sYear + "-07-01";
		} else {
			return sYear + "-10-01";
		}
	}

	/**
	 * 通过年份和季度数返回当前季度的截止日期
	 * 
	 * @param iMonth
	 *            月份
	 * @return 季度数
	 */
	public static String getSeasonEDate(String sYear, int iSeason) {
		if (iSeason < 0 || iSeason > 4)
			return "";
		if (iSeason == 1) {
			return sYear + "-03-31";
		} else if (iSeason == 2) {
			return sYear + "-06-30";
		} else if (iSeason == 3) {
			return sYear + "-09-30";
		} else {
			return sYear + "-12-31";
		}
	}

	/**
	 * 防止sql注入，对输入参数进行过滤
	 * 
	 * @param String
	 *            inputString 需要过滤的字符串
	 * @return String 返回过滤后的字符串
	 */
	public static String cleanString(String inputString) {
		StringBuilder retVal = new StringBuilder();
		if ((inputString != null) && (!inputString.equals(""))) {
			inputString = inputString.trim();
			for (int i = 0; i < inputString.length(); i++) {
				String sub = inputString.substring(i, i + 1);
				if (sub.equals("\"") || sub.equals("<") || sub.equals(">")
						|| sub.equals("\'") || sub.equals("=")) {
					retVal.append("");
				} else {
					retVal.append(sub);
				}
			}
		}
		return retVal.toString().replace("'", "");
	}

	/**
	 * 去除掉Object[]为null的条目 对象数组里面的数据类型，暂时只支持两种，int和string
	 * 
	 * @param String
	 *            inputString 需要过滤的字符串
	 * @return String 返回过滤后的字符串
	 */
	public static Object[] cleanObjects(Object[] parameters) {
		if (parameters == null) {
			return null;
		}
		int iLen = 0;
		int i = 0;
		for (; i < parameters.length; i++) {
			if (parameters[i] != null) {
				iLen++;
			}
		}
		Object[] retParam = new Object[iLen];
		for (i = 0; i < iLen; i++) {
			if (parameters[i] instanceof Integer) {
				retParam[i] = Integer.valueOf(((Integer) parameters[i])
						.intValue());
			} else {
				retParam[i] = (String) parameters[i];
			}
		}
		return retParam;
	}

	/**
	 * 转换为货币类型-默认保留两位的千分符的方法
	 * 
	 * @param String
	 *            str 需要转换为货币类型的字符串
	 * @return String 返回过滤后的字符串
	 */
	public static String getCurrency(String str) {
		if (str == null || str.equals("")) {
			return "";
		}
		double d;
		try {
			d = Double.parseDouble(str);
			if (d == 0) {
				return new DecimalFormat("0.00").format(d);
			} else {
				return new DecimalFormat(",###.00").format(d);
			}
		} catch (Exception ex) {
			return "";
		}
	}

	/**
	 * 转换为货币类型－转换为指定小数位数的千分符的方法
	 * 
	 * @param String
	 *            str 需要转换为货币类型的字符串
	 * @param int iDecimalLen 显示的小数位数
	 * @return String 返回过滤后的字符串
	 */
	public static String getCurrency(String str, int iDecimalLen) {
		if (str == null || str.equals("")) {
			return "";
		}
		double d;
		try {
			d = Double.parseDouble(str);
			StringBuffer sbFormat = new StringBuffer();
			for (int i = 0; i < iDecimalLen; i++) {
				sbFormat.append("0");
			}
			if (iDecimalLen == 0) {
				if (d == 0) {
					return new DecimalFormat("0").format(d);
				} else {
					return new DecimalFormat(",###").format(d);
				}
			} else {
				if (d == 0) {
					return new DecimalFormat("0." + sbFormat.toString())
							.format(d);
				} else {
					return new DecimalFormat(",###." + sbFormat.toString())
							.format(d);
				}
			}

		} catch (Exception ex) {
			return "";
		}
	}

	/**
	 * 取服务器当前的时间戳，毫秒级（yyyyMMddHHmmssSSS）
	 * 
	 * @return String 返回服务器当前的时间戳字符串
	 */
	public static String getServerTimePoke() {
		return new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date(System
				.currentTimeMillis()));
	}

	/**
	 * 根据传入年月,返回某年某月的最后一天.
	 * 
	 * @param zq
	 *            传入年月(如2009-02)
	 * @return
	 * @throws ParseException
	 */
	public static String getEndOfMonth(String zq) throws ParseException {
		Date date = new SimpleDateFormat("yyyy-MM-dd").parse(zq + "-01");
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return zq + "-" + cal.getActualMaximum(Calendar.DAY_OF_MONTH);
	}
}