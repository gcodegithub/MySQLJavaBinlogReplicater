package cn.ce.utils.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.validator.EmailValidator;

public class BeanUtil {

	private final static Log logger = LogFactory.getLog(BeanUtil.class);

	public static Long getLastAvailMem() {
		Long free = Runtime.getRuntime().freeMemory();
		Long total = Runtime.getRuntime().totalMemory();
		Long max = Runtime.getRuntime().maxMemory();
		if (total.equals(max)) {
			return free;
		}
		return max - total + free;
	}

	public static void seriObject2File(String fileFullPath, Serializable obj)
			throws Exception {
		File outFile = new File(fileFullPath);
		ObjectOutputStream output = new ObjectOutputStream(
				new FileOutputStream(outFile));
		output.writeObject(obj);
		IOUtils.closeQuietly(output);
		ProFileUtil.checkIsExist(fileFullPath, true);
	}

	public static Serializable getSeriObjFromFile(String fileFullPath)
			throws Exception {
		ProFileUtil.checkIsExist(fileFullPath, true);
		ObjectInputStream input = new ObjectInputStream(new FileInputStream(
				new File(fileFullPath)));
		Serializable t = (Serializable) input.readObject();
		IOUtils.closeQuietly(input);
		return t;
	}

	public static String getFirstOneFromCSV(String csv, String token)
			throws RuntimeException {
		List<String> list = BeanUtil.csvToList(csv, token);
		if (list.get(0) == null) {
			throw new RuntimeException("Error:输入的参数csv首个元素为空!");
		}
		return list.get(0);

	}

	// 将无序的Collection排序成有序的List返回，根据getMethodName的返回值的大小ASC排序
	public static <T> List<T> getASCOrderByGetMethod(Collection<T> collection,
			final String getMethodName) {
		if (collection == null || collection.size() == 0) {
			return null;
		}
		List<T> tempList = new ArrayList<T>(collection);
		Collections.sort(tempList, new Comparator<T>() {
			public int compare(T t1, T t2) {
				Class t1OwnerClass = t1.getClass();
				Class t2OwnerClass = t2.getClass();
				String res1 = null;
				String res2 = null;
				try {
					Method getMethodNameM1 = t1OwnerClass.getDeclaredMethod(
							getMethodName, new Class[] {});
					Method getMethodNameM2 = t2OwnerClass.getDeclaredMethod(
							getMethodName, new Class[] {});
					res1 = getMethodNameM1.invoke(t1).toString();
					res2 = getMethodNameM2.invoke(t2).toString();
				} catch (Exception e) {

					e.printStackTrace();
				}
				if (res1 == null || res2 == null) {
					throw new RuntimeException("无法进行比较");
				}
				Integer resInt1 = Integer.valueOf(res1);
				Integer resInt2 = Integer.valueOf(res2);
				return resInt1 - resInt2;
			}
		});
		return tempList;
	}

	// 根据集合中对象的get方法返回值，按顺序返回get方法返回值的csv字符串
	public static <T> String getGetMethodValueCSVByList(List<T> objectList,
			final String getMethodName, final String separator)
			throws Exception {
		if (objectList == null || objectList.size() == 0) {
			return null;
		}
		if (StringUtils.isBlank(separator)) {
			throw new Exception("分割符不能为空");
		}
		StringBuilder sb = new StringBuilder();
		for (T t : objectList) {
			Class tOwnerClass = t.getClass();
			Method getMethodNameM = tOwnerClass.getDeclaredMethod(
					getMethodName, new Class[] {});
			String res = getMethodNameM.invoke(t).toString();
			sb.append(res);
			sb.append(separator);
		}
		String s = sb.toString();
		s = s.substring(0, s.length() - 1);
		logger.info("the csv is " + s);
		return s;
	}

	public static boolean isInCSV(String target, String csv, String token)
			throws Exception {
		if (target == null || target.trim().length() == 0) {
			throw new Exception("Error:isInCSV方法参数中输入的目标字符串为空!");
		}
		boolean isInCSV = false;
		List<String> csvList = BeanUtil.csvToList(csv, token);
		for (String tmp : csvList) {
			if (target.equals(tmp)) {
				isInCSV = true;
				break;
			}
		}
		return isInCSV;

	}

	// csv格式转换成List,
	public static List<String> csvToList(String csv, String token)
			throws RuntimeException {
		if (csv == null || csv.trim().length() == 0) {
			throw new RuntimeException("Error:输入的参数csv为空!");
		}
		List<String> res = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(csv, token);
		while (tokenizer.hasMoreElements()) {
			String value = tokenizer.nextToken();
			value = value.trim();
			res.add(value);
		}
		if (res.size() == 0 || res.get(0) == null) {
			return null;
		}
		return res;
	}

	public static <T> String listToCSV(List<T> list, String token) {
		if (CollectionUtils.isEmpty(list)) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		for (T t : list) {
			sb.append(token);
			sb.append(t.toString());
		}
		int tokenLen = token.length();
		String resCSV = sb.substring(tokenLen);
		return resCSV;
	}

	// bean 的proNames属性不为null
	public static <T> void proNotNull(T bean, String[] proNames)
			throws Exception {
		if (proNames == null || proNames.length == 0
				|| proNames[0].trim().length() == 0) {
			throw new Exception("Error:输入的参数proNamesw为空!");
		}
		List<String> proList = Arrays.asList(proNames);
		Map<String, ?> beanMap = new BeanMap(bean);
		for (String proName : proList) {
			if (beanMap.get(proName) == null) {
				throw new Exception("Error:CusRegisterVO属性" + proName + "不能为空!");
			}
			String value = beanMap.get(proName).toString();
			boolean isBlank = StringUtils.isBlank(value);
			if (isBlank) {
				throw new Exception("Error:CusRegisterVO属性" + proName + "不能为空!");
			}
		}
	}

	// bean 的proNamesEmail属性是否合法
	public static <T> void proEmailValid(T bean, String[] proNames)
			throws Exception {
		if (proNames == null || proNames.length == 0
				|| proNames[0].trim().length() == 0) {
			throw new Exception("Error:输入的参数proNamesw为空!");
		}
		List<String> proList = Arrays.asList(proNames);
		Map<String, ?> beanMap = new BeanMap(bean);
		for (String proName : proList) {
			String value = beanMap.get(proName).toString();
			// Get an EmailValidator
			EmailValidator validator = EmailValidator.getInstance();
			// Validate an email address
			boolean isAddressValid = validator.isValid(value);
			if (!isAddressValid) {
				throw new Exception("Error:CusRegisterVO " + value
						+ "是一个非法的Email地址!");
			}
		}
	}//

	// private static final java.lang.String DOMAIN_PATTERN =
	// "/^[^\s\(\)<>@,;:'\\\"\.\[\]]+(\.[^\s\(\)<>@,;:'\\\"\.\[\]]+)*\s*$/";
	// bean 的域名属性是否合法 比如 abc.com
	public static <T> void proDomainValid(T bean, String[] proNames)
			throws Exception {
		if (proNames == null || proNames.length == 0
				|| proNames[0].trim().length() == 0) {
			throw new Exception("Error:输入的参数proNames为空!");
		}
		List<String> proList = Arrays.asList(proNames);
		Map<String, ?> beanMap = new BeanMap(bean);
		for (String proName : proList) {
			String value = beanMap.get(proName).toString();
			String check = "([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";
			Pattern regex = Pattern.compile(check);
			Matcher matcher = regex.matcher(value);
			boolean isValid = matcher.matches();
			if (!isValid) {
				throw new Exception("Error:CusRegisterVO " + value
						+ "是一个非法的域名属性!");
			}
		}
	}//

	public static String getErrorDetail(Throwable e) {
		String errorInfo = "\n" + e.getMessage() + "\n" + e.getClass();
		// StackTraceElement[] errEle = e.getStackTrace();
		// List errEleList = BeanUtil.arrayToList(errEle);
		// errorInfo = errorInfo + errEleList.toString();
		// e = e.getCause();
		// if (e != null) {
		// errorInfo = errorInfo + BeanUtil.getErrorDetail(e);
		// }
		return errorInfo;
	}

	public static <T> List<T> arrayToList(T[] array) {
		List<T> res = new ArrayList<T>();
		for (T t : array) {
			res.add(t);
		}
		return res;
	}

	public static void main(String[] args) {

		try {
			// list = BeanUtil.csvToList(";", ";");
			// int size = list.size();
			// System.out.println(size);
			// boolean isIn = BeanUtil
			// .isInCSV(
			// "www.abc.com",
			// "www.abc.cn,abc.cn,www.abc.com.cn,www.abc.com,abc.com",
			// ",");
			// System.out.println(isIn);
			String csv = "abc,bcd,def";
			String token = ",";
			List list = BeanUtil.csvToList(csv, token);
			csv = BeanUtil.listToCSV(list, token);
			System.out.println(csv);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
