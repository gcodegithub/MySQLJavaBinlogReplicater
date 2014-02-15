package cn.ce.utils.filecache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileObjectCacheOperUtil {
	private final static Log logger = LogFactory
			.getLog(FileObjectCacheOperUtil.class);

	// 存文件到磁盘,并纳入缓存
	public static <T> void saveCacheObject2File(T t, String fullPath,
			IFileObjectCache2StreamConverter converter) throws Exception {
		// 进行持久化
		BufferedOutputStream bo = null;
		try {
			File file = new File(fullPath);
			FileOutputStream fo = new FileOutputStream(file);
			bo = new BufferedOutputStream(fo);
			converter.FileObject2stream(t, bo);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			IOUtils.closeQuietly(bo);
		}
		// 校验是否文件写入到磁盘
		File savedFile = new File(fullPath);
		Boolean isSaved = savedFile.exists();
		if (!isSaved) {
			throw new Exception("Error:无法持久化文件，请检查读写权限、文件路径!");
		}
		//logger.info("将对象" + t + "序列化到文件:" + fullPath);
		// 持久化后纳入缓存
		FileObjectCacheManager.setCGInfoDescByCache(fullPath, t);
	}

	// 从读取文件并纳入缓存
	@SuppressWarnings("unchecked")
	public static <T> T loadObject(String fullPath,
			IStream2FileObjectCacheConverter converter) throws Exception {
		if (fullPath == null || fullPath.length() == 0) {
			throw new Exception("Error:input parameter is null or empty!");
		}
		// 先尝试从缓存读取
		T fileObject = (T) FileObjectCacheManager
				.getFileObjectByCache(fullPath);
		if (fileObject != null) {
			//logger.debug("从缓存中返回的对象为" + fileObject + "对应路径为：" + fullPath);
			return (T) fileObject;
		}
		// 缓存中不存在或者缓存中内容过期，就从文件中解析
		FileInputStream fin = null;
		BufferedInputStream bi = null;
		try {
			File file = new File(fullPath);
			fin = new FileInputStream(file);
			bi = new BufferedInputStream(fin);
			fileObject = (T) converter.stream2FileObject(bi);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			IOUtils.closeQuietly(bi);
		}
		if (fileObject == null) {
			throw new Exception("无法从文件：" + fullPath + "中打开输入流，检查文件是否存在、文件权限等！");
		}
		// 解析完成后缓存该对象对对象进行校验
		converter.validateConvert(fileObject);
		FileObjectCacheManager.setCGInfoDescByCache(fullPath, fileObject);
		//logger.info("解析" + fullPath + "返回的对象为" + fileObject);
		return (T) fileObject;
	}
}
