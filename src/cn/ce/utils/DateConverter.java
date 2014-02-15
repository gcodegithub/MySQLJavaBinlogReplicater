package cn.ce.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DateConverter {
	private static final DateFormat[] ACCEPT_DATE_FORMATS = {
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
			new SimpleDateFormat("yyyy-MM-dd"),
			new SimpleDateFormat("yyyy/MM/dd") }; // 支持转换的日期格式

	public static void main(String[] args) {
		DateConverter.milli2HourMinSec(900000l);
	}

	public static String milli2HourMinSec(Long millis, String... param) {
		// long millis = 900000;
		if (param.length == 0) {
			param = new String[] { "%d hour,%d min, %d sec" };
		}
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		long minus = TimeUnit.MILLISECONDS.toMinutes(millis) - hours * 60;
		long sec = TimeUnit.MILLISECONDS.toSeconds(millis) - minus * 60 - hours
				* 60 * 60;
		String s = String.format(param[0], hours, minus, sec);
		return s;
	}

}