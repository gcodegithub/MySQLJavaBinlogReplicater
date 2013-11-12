package cn.ce.utils.filecache;

import java.io.File;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileObjectCacheManager {
	private final static Log logger = LogFactory
			.getLog(FileObjectCacheManager.class);
	private static MyThreadLocalMap cacheMap = new MyThreadLocalMap();

	// 读取缓存对象,如果有缓存则给出缓存,没缓存或者缓存过期返回null
	public static <T> T getFileObjectByCache(String inFullFilePath)
			throws Exception {
		File file = new File(inFullFilePath);
		long nowModifiedTime = file.lastModified();
		long nowSize = file.length();
		// 取出缓存的描述
		FileObjectCacheDesc<T> fileObjectDesc = (FileObjectCacheDesc<T>) cacheMap
				.getMap().get(inFullFilePath);
		if (fileObjectDesc == null) {
			return null;
		}
		long lastModifiedTime = fileObjectDesc.getLastModifiedTime();
		long lastSize = fileObjectDesc.getLastSize();
		String cacheFileFullPath = fileObjectDesc.getFileFullPath();
		T cacheObject = (T) fileObjectDesc.getCacheObject();
		if (!inFullFilePath.equals(cacheFileFullPath)) {
			String errorMSG = "Error:" + inFullFilePath
					+ "为Key的缓存对象内容和该路径不符，致命错误！";
			throw new Exception(errorMSG);
		}
		// 缓存过期，清除缓存
		if ((nowModifiedTime != lastModifiedTime) || (nowSize != lastSize)) {
			cacheMap.getMap().remove(inFullFilePath);
			logger.info("缓存中的对象" + cacheObject + "已经过期，从缓存中清除");
			return null;
		}
		return (T) cacheObject;
	}

	// 将对象缓存起来
	public static <T> void setCGInfoDescByCache(String fileFullPath,
			T fileObject) throws Exception {
		File xmlFile = new File(fileFullPath);
		long nowModifiedTime = xmlFile.lastModified();
		long nowSize = xmlFile.length();
		FileObjectCacheDesc<T> fileObjectDesc = new FileObjectCacheDesc<T>();
		fileObjectDesc.setCacheObject(fileObject);
		fileObjectDesc.setFileFullPath(fileFullPath);
		fileObjectDesc.setLastSize(nowSize);
		fileObjectDesc.setLastModifiedTime(nowModifiedTime);
		cacheMap.getMap().put(fileFullPath, fileObjectDesc);
		logger.info("MSG:对" + fileObject + "对象进行缓存，对应fileFullPath="
				+ fileFullPath);
	}

	// -----------------------------------私有
	private static class MyThreadLocalMap<T> extends ThreadLocal {
		public Object initialValue() {
			return new WeakHashMap<String, FileObjectCacheDesc<T>>();
		}

		public Map<String, FileObjectCacheDesc<T>> getMap() {
			return (Map<String, FileObjectCacheDesc<T>>) super.get();
		}

		public void remove() {
			this.getMap().clear();
			super.remove();
		}
	}

	public static void main(String args[]) {

	}

}
