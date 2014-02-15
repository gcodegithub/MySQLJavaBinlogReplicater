package cn.ce.utils.filecache;

import java.io.InputStream;

public interface IStream2FileObjectCacheConverter {
	public <T> T stream2FileObject(InputStream inputStream) throws Exception;

	// 读取后的校验
	public <T> void validateConvert(T t) throws Exception;
}
