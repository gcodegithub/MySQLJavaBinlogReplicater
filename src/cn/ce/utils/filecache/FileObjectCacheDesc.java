package cn.ce.utils.filecache;

public class FileObjectCacheDesc<T> {
	private long lastModifiedTime = 0;
	private long lastSize = 0;
	private String fileFullPath;
	private T cacheObject;

	public long getLastModifiedTime() {
		return lastModifiedTime;
	}

	public void setLastModifiedTime(long lastModifiedTime) {
		this.lastModifiedTime = lastModifiedTime;
	}

	public long getLastSize() {
		return lastSize;
	}

	public void setLastSize(long lastSize) {
		this.lastSize = lastSize;
	}

	public String getFileFullPath() {
		return fileFullPath;
	}

	public void setFileFullPath(String fileFullPath) {
		this.fileFullPath = fileFullPath;
	}

	public T getCacheObject() {
		return cacheObject;
	}

	public void setCacheObject(T cacheObject) {
		this.cacheObject = cacheObject;
	}
}
