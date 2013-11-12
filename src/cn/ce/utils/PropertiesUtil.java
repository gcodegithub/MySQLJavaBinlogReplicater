package cn.ce.utils;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import cn.ce.utils.filecache.FileObjectCacheOperUtil;
import cn.ce.utils.filecache.PropertiesConverter;


public class PropertiesUtil {
	public static String getValueByKey(String fullPath, String key)
			throws Exception {
		if (StringUtils.isBlank(fullPath) || StringUtils.isBlank(key)) {
			throw new Exception("提取资源文件值方法的输入的参数为空值");
		}
		Properties p = FileObjectCacheOperUtil.loadObject(fullPath,
				new PropertiesConverter());
		String value = p.getProperty(key);
		if (StringUtils.isBlank(value)) {
			throw new RuntimeException("请检查部署环境！key在资源文件中内容为空！资源文件路径:"
					+ fullPath + " 资源key为:" + key);
		}
		return value;
	}
	
	public static String getValueByClasspath(String fileName,String key){
		String value="";
		try {
			Resource resource = new ClassPathResource(fileName);
			String fullFilePath=resource.getFile().getCanonicalPath();
			value = PropertiesUtil.getValueByKey(fullFilePath,key);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	public static void main(String[] args) {
		String value;
		try {
			Resource resource = new ClassPathResource("url.properties");
			String fullFilePath=resource.getFile().getCanonicalPath();
			value = PropertiesUtil.getValueByKey(fullFilePath,
					"read_timeout");
			System.out.print(value);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
