package cn.ce.utils.common;

import java.io.File;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LinuxUtil {
	private final static Log logger = LogFactory.getLog(LinuxUtil.class);

	public static void runLinuxLocalCommond(List<String> cmdsList)
			throws Exception {
		// 默认超时时间为30分钟
		runLinuxLocalCommond(cmdsList, 30);
	}

	public static Set<InetAddress> getLocalIps() throws SocketException {
		Set<InetAddress> allips = new HashSet<InetAddress>();
		Enumeration<NetworkInterface> netInterfaces = NetworkInterface
				.getNetworkInterfaces();
		while (netInterfaces.hasMoreElements()) {
			NetworkInterface ni = netInterfaces.nextElement();
			Enumeration<InetAddress> ips = ni.getInetAddresses();
			while (ips.hasMoreElements()) {
				InetAddress ip = ips.nextElement();
				allips.add(ip);
			}
		}
		return allips;
	}

	public static void runLinuxLocalCommond(List<String> cmdsList, int minute)
			throws Exception {
		if (cmdsList == null || cmdsList.size() <= 0) {
			throw new Exception("linux命令至少需要有一个命令！");
		}
		logger.info("运行的命令为：" + cmdsList);
		String executable = cmdsList.get(0);
		CommandLine cmdLine = new CommandLine(executable);
		for (String arg : cmdsList) {
			if (!arg.equals(cmdsList.get(0)))
				cmdLine.addArgument(arg);
		}
		if (minute <= 0) {
			throw new Exception("超时时间必须大于1分钟");
		}
		// 超时时间 30分钟
		ExecuteWatchdog watchdog = new ExecuteWatchdog(minute * 60 * 1000);
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
		MyFilterOutputStream error = new MyFilterOutputStream(System.err);
		PumpStreamHandler streamHandler = new PumpStreamHandler(System.out,
				error);
		Executor executor = new DefaultExecutor();
		executor.setWorkingDirectory(new File("/"));
		executor.setWatchdog(watchdog);
		executor.setExitValue(0);
		executor.setStreamHandler(streamHandler);
		executor.execute(cmdLine, resultHandler);
		resultHandler.waitFor();
		ExecuteException ex = resultHandler.getException();
		int returnValue = resultHandler.getExitValue();
		boolean hasErrOut = error.isHasContent();
		if (ex != null || returnValue != 0 || hasErrOut) {
			logger.error(ex);
			String errMsg = error.getErrorString();
			if (ex != null) {
				errMsg = ex.getMessage() + " " + errMsg;
			}
			throw new Exception("执行Linux命令失败!命令为=" + cmdsList + " 异常信息:"
					+ errMsg);
		}
	}

	private static void testRsync() throws Exception {
		List cmdList = new ArrayList();
		cmdList.add("rsync");
		cmdList.add("-aviz");
		cmdList.add("--password-file=/etc/rsyncd.secrets");
		cmdList.add("--log-file=/home/hades/mytestlog.log");
		cmdList.add("zshop@172.20.88.78::cluster/filePath.xml");
		cmdList.add("/home/hades/");
		LinuxUtil.runLinuxLocalCommond(cmdList);
		System.out.println("-----------------OVER----");
	}

	public static void main(String[] args) {
		try {
			LinuxUtil.testRsync();
			System.out.println("-----------------OVER----");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
