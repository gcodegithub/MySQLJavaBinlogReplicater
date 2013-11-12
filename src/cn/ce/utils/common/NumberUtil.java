package cn.ce.utils.common;

import org.apache.commons.lang.RandomStringUtils;

public class NumberUtil {
	public static Long getRandomLong(int lenth) {
		String ran = RandomStringUtils.random(lenth, false, true);
		Long value = Long.parseLong(ran);
		return value;
	}
}
