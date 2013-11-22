package cn.ce.utils.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import cn.ce.utils.filecache.FileObjectCacheOperUtil;
import cn.ce.utils.filecache.PropertiesConverter;

public class ProFileUtil {

	private final static Log logger = LogFactory.getLog(ProFileUtil.class);

	// ------------
	public static String findMsgString(String proFileClassPath, Object keyO)
			throws Exception {
		String key = String.valueOf(keyO);
		return ProFileUtil.getValueFromProInClassPath(proFileClassPath, key);
	}

	public static TreeMap<String, String> findMsgMap(String proFileClassPath)
			throws Exception {
		String absPath = ProFileUtil.getFileAbsPath(proFileClassPath);
		ProFileUtil.checkIsExist(absPath, true);
		Properties p = ProFileUtil.loadFromCGInfoCache(absPath);
		TreeMap hm = new TreeMap<String, String>();
		hm.putAll(p);
		return hm;
	}

	public static <T> void genSQLFile(String sqlFilePath, String vmFileDirPath,
			String vmFileName, Set<T> dataInfoSet, String dataInfoSetkey)
			throws Exception {
		ProFileUtil.checkIsExist(vmFileDirPath + vmFileName, true);
		FileUtils.touch(new File(sqlFilePath));
		// 初始化并取得Velocity引擎
		VelocityEngine ve = new VelocityEngine();
		Properties properties = new Properties();
		// 将当前路径设置到VelocityEngine 中
		properties.setProperty(Velocity.FILE_RESOURCE_LOADER_PATH,
				vmFileDirPath);
		properties.setProperty(Velocity.ENCODING_DEFAULT, "UTF-8");
		properties.setProperty(Velocity.INPUT_ENCODING, "UTF-8");
		properties.setProperty(Velocity.OUTPUT_ENCODING, "UTF-8");
		properties.setProperty(Velocity.SET_NULL_ALLOWED, "true");
		ve.init(properties);
		// 取得velocity的模版
		Template t = ve.getTemplate(vmFileName);
		// 取得velocity的上下文context
		VelocityContext context = new VelocityContext();
		// 把数据填入上下文
		context.put(dataInfoSetkey, dataInfoSet);
		// 输出流
		FileWriter writer = new FileWriter(sqlFilePath);
		// 转换输出
		t.merge(context, writer);
		IOUtils.closeQuietly(writer);
		ProFileUtil.checkIsExist(sqlFilePath, true);
	}

	public static void appendFile(String filePath, String content)
			throws IOException {
		File file = new File(filePath);
		Boolean isFile = file.isFile();
		if (!isFile) {
			FileUtils.touch(file);
		}
		// 追加内容
		FileWriter fw = null;
		try {
			fw = new FileWriter(filePath, true);
			fw.write(content);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			if (fw != null) {
				fw.close();
			}

		}
	}

	public static void mkdir(String mkDirPath) throws Exception {
		List<String> cmdsList = new ArrayList<String>();
		cmdsList.add("mkdir");
		cmdsList.add("-p");
		cmdsList.add(mkDirPath);
		LinuxUtil.runLinuxLocalCommond(cmdsList);
		ProFileUtil.checkIsExist(mkDirPath, false);
	}

	// 检测本地文件或者目录是否存在
	public static void checkIsExist(String fullFileLocalPath, boolean isFile)
			throws Exception {
		File file = new File(fullFileLocalPath);
		if (isFile) {
			if (!file.canRead()) {
				throw new Exception("文件不存在，或者没有权限读取，路径为:" + fullFileLocalPath);
			}
		} else {
			if (!file.isDirectory()) {
				throw new Exception("目录不存在，或者没有权限读取，路径为:" + fullFileLocalPath);
			}
		}
	}

	public static String getValueFromProInClassPath(String proFileClassPath,
			String key) throws Exception {
		String absPath = ProFileUtil.getFileAbsPath(proFileClassPath);
		ProFileUtil.checkIsExist(absPath, true);
		Properties p = ProFileUtil.loadFromCGInfoCache(absPath);
		String value = (String) p.get(key);
		return value;
	}

	public static String getValueFromProAbsPath(String absPath, String key)
			throws Exception {
		ProFileUtil.checkIsExist(absPath, true);
		Properties p = ProFileUtil.loadFromCGInfoCache(absPath);
		String value = (String) p.get(key);
		return value;
	}

	public static String getFileAbsPath(String fileClassPath)
			throws IOException {
		Resource res = new ClassPathResource(fileClassPath);
		URL url = res.getURL();
		String absPath = url.getFile();
		return absPath;
	}

	public static URL getURL(String classPath) throws IOException {
		Resource res = new ClassPathResource(classPath);
		URL url = res.getURL();
		return url;
	}

	public static Properties loadFromCGInfoCache(String localProPath)
			throws Exception {
		boolean isFile = true;
		ProFileUtil.checkIsExist(localProPath, isFile);
		Properties properties = FileObjectCacheOperUtil.loadObject(
				localProPath, new PropertiesConverter());
		return properties;
	}

	// 文件所有内容清空为新值
	// 如果文件不存在则创建该文件
	public static void clearOrCreateFile(String localFilePath, String newText,
			String encoding) throws IOException {
		File file = new File(localFilePath);
		Boolean isFile = file.isFile();
		if (!isFile) {
			FileUtils.touch(file);
		}
		FileUtils.writeStringToFile(file, newText, encoding);

	}

	public static void replaceTextInFile(String localFilePath, String oldText,
			String newText, String encoding) throws Exception {
		File file = new File(localFilePath);
		boolean isFile = true;
		ProFileUtil.checkIsExist(localFilePath, isFile);
		String fileContent = FileUtils.readFileToString(file, encoding);
		fileContent = fileContent.replaceAll(oldText, newText);
		FileUtils.writeStringToFile(file, fileContent, encoding);

	}

	// 普通文件修改，修改或者删除普通文件某一行对应数据
	// 注意：文件所有內容都會加載入內存！
	// 注意：避免多進程併發，只保證綫程安全
	public static void modifyOrCreateRowRec(String localFilePath,
			String rowContent, String fileEncoding, boolean isDelete)
			throws Exception {
		File oldFile = new File(localFilePath);
		boolean isFile = true;
		ProFileUtil.checkIsExist(localFilePath, isFile);
		File tempFile = new File(oldFile.getAbsolutePath() + ".tmp"
				+ System.currentTimeMillis());
		LineIterator it = FileUtils.lineIterator(oldFile, fileEncoding);
		// 留下来的行
		List<String> linesRemain = new ArrayList<String>();
		try {
			while (it.hasNext()) {
				String line = it.nextLine();
				if (isDelete) {
					// 如果是删除的情况
					if (StringUtils.equals(line, rowContent)) {
						continue;
					}
				}
				linesRemain.add(line);
			}
			if (!isDelete) {
				// 如果是增加的情况
				linesRemain.add(rowContent);
			}
		} finally {
			LineIterator.closeQuietly(it);
		}

		FileUtils.writeLines(tempFile, fileEncoding, linesRemain,
				System.getProperty("line.separator"));
		isFile = tempFile.isFile() && tempFile.exists();
		if (!isFile) {
			throw new Exception("Error:临时文件无法写入检查权限！");
		}
		FileInputStream fileIn = new FileInputStream(tempFile);
		FileOutputStream fileOut = new FileOutputStream(localFilePath);
		// -------旧文件更新
		IOUtils.copy(fileIn, fileOut);
		IOUtils.closeQuietly(fileIn);
		IOUtils.closeQuietly(fileOut);
		// -------临时文件删除
		tempFile.delete();
	}

	// 带文件锁增加，修改或者删除资源文件某个记录
	// 注意：文件所有內容都會加載入內存！
	public synchronized static void modifyOrCreatePropertiesWithFileLock(
			String localFilePath, String key, String value, boolean isDelete,
			boolean isReWriteMapFormat) throws Exception {
		if (StringUtils.isBlank(localFilePath) || StringUtils.isBlank(key)
				|| StringUtils.isBlank(value)) {
			throw new Exception("修改本地资源文件方法输入的所有参数均不能为空！");
		}
		File propertiesFile = new File(localFilePath);
		if (!propertiesFile.exists()) {
			logger.warn("输入资源文件不存在！其绝对路径为：" + localFilePath + " 将创建该文件");
			boolean isOk = propertiesFile.createNewFile();
			if (!isOk) {
				throw new Exception("无法创建资源文件，请检查权限" + localFilePath);
			}
		}

		FileLock fileLock = null;
		// 内存的资源文件镜像
		PropertiesExtend p = new PropertiesExtend();
		InputStream fin = null;
		OutputStream fou = null;
		RandomAccessFile accessFile = new RandomAccessFile(localFilePath, "rw");
		FileChannel fileChannel = null;
		// 尝试获取锁次数
		AtomicInteger count = new AtomicInteger(0);
		try {
			// ---------------------------获取文件锁
			fileChannel = accessFile.getChannel();
			logger.info("准备获取文件锁");
			// 准备获取锁，超时会退出
			while (true) {
				fileLock = fileChannel.tryLock();
				// 最大等待时间为30秒
				if (fileLock == null && count.intValue() >= 15) {
					// 超时了
					throw new Exception("等待超时，退出");
				}
				if (fileLock != null) {
					break;
				}
				Long ranSleep = NumberUtil.getRandomLong(3);
				logger.info("准备休息毫秒数=" + ranSleep);
				// 等待时间在0.1秒～0.9秒间
				Thread.currentThread().sleep(ranSleep);
				count.incrementAndGet();
			}
			logger.info("获取的文件锁=" + fileLock);
			// 表示已经获取锁
			// ---------------------------
			fin = new RAFInputStream(accessFile);
			fou = new RAFOutputStream(accessFile);
			p.load(fin);
			logger.info("准备读出文件" + localFilePath);
			logger.info("写之前的资源文件内容:" + p);
			logger.info("准备写入记录:" + key + "=" + value);
			if (isDelete) {
				p.remove(key);
			} else {
				p.setProperty(key, value);
			}

			// 保存
			logger.info("准备保存");
			accessFile.seek(0);
			if (isReWriteMapFormat) {
				p.store(fou, "edit success", " ");
			} else {
				p.store(fou, "edit success");
			}
			fou.flush();
			long fileSize = ((RAFOutputStream) fou).getLength();
			accessFile.setLength(fileSize);
			logger.info("保存成功");
		} finally {
			if (fileLock != null) {
				fileLock.release();
			}
			IOUtils.closeQuietly(fin);
			IOUtils.closeQuietly(fou);
			logger.info("文件操作完毕");
		}

	}

	public static void main(String[] args) {
		try {
			ProFileUtil.appendFile("/home/hades/temp/rsyncLog.txt", "abcdefg");
			System.out.println("------------------OK---------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
