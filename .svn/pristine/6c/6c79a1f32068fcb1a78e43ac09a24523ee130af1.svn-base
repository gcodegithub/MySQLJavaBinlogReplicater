package cn.ce.utils.common;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class StringUtil {
	public final static String EMPTY = "";
	public final static int ENTER = 1;//回车
	public final static int COMMA = 0;//逗号
	private static final Log log = LogFactory.getLog(StringUtil.class);
	
	public static String format(String str, Object... args) 
	{		
		if (str == null || "".equals(str))
			return EMPTY;
		if (args.length == 0) {
			return str;
		}		
		try
		{
			String result = str;		
			java.util.regex.Pattern p = java.util.regex.Pattern
					.compile("\\{(\\d+)\\}");
			java.util.regex.Matcher m = p.matcher(str);			
			while (m.find()) 
			{			
				int index = Integer.parseInt(m.group(1));			
				if (index < args.length) 
				{				
					result = result.replace(m.group(), args[index].toString());
				}
			}
			return result;
		}
		catch(Exception e)
		{
			log.info("string format failed.");
		}		
		return EMPTY;
	}
	
	public static Boolean isBlank(String src)
	{
		if(null==src)
		{
			return true;
		}
		
		if("".equals(src))
		{
			return true;
		}
		
		return false;
	}
	
}
