package cn.ce.utils.filecache;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesConverter implements IStream2FileObjectCacheConverter {

	public Properties stream2FileObject(InputStream inputStream)
			throws Exception {
		Properties p = new Properties();
		p.load(inputStream);
		return p;
	}

	public <T> void validateConvert(T t) throws Exception {
		// 无须校验
	}

}
