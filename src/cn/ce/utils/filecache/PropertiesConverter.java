package cn.ce.utils.filecache;

import java.io.InputStream;

import cn.ce.utils.common.PropertiesExtend;

public class PropertiesConverter implements IStream2FileObjectCacheConverter {

	public PropertiesExtend stream2FileObject(InputStream inputStream)
			throws Exception {
		PropertiesExtend p = new PropertiesExtend();
		p.load(inputStream);
		return p;
	}

	public <T> void validateConvert(T t) throws Exception {
		// 无须校验
	}

}
