package cn.ce.cons;

public class Const {
	public static final String OK = "OK";
	public static final String ERROR = "ERROR";
	public static final String NEW_TOKEN = "NEW_TOKEN";
	public static final String sysconfigFileClasspath = "conf/sysconfig.properties";

	public static int poolSize = 10;
	public static int cpuNum = Runtime.getRuntime().availableProcessors();
	public static final String filterFileClasspath = "conf/filter.properties";
	public static final String UPDATE = "UPDATE";
	public static final String INSERT = "INSERT";
	public static final String DELETE = "DELETE";
	public static final String UPDATE_PART = "UPDATE_PART";

	static {
		if (poolSize < Const.cpuNum) {
			poolSize = Const.cpuNum * 2;
		}

	}
}
