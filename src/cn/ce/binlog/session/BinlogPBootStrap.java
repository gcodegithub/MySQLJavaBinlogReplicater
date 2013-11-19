package cn.ce.binlog.session;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.utils.JaxbContextUtil;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.mail.Alarm;
import cn.ce.web.rest.vo.BinParseResultVO;
import cn.ce.web.rest.vo.EventVO;

public class BinlogPBootStrap implements InitializingBean, DisposableBean {

	private final static Log logger = LogFactory.getLog(BinlogPBootStrap.class);

	private static final String sysconfigFileClasspath = "conf/sysconfig.properties";
	private static final String checkpointFileClasspath = "conf/clientIdToken.properties";
	private static MysqlConnector c;
	private String serverhost;
	private String serverPort;
	private String username;
	private String password;

	private static final BinlogParseSession bps = new BinlogParseSession();

	public void afterPropertiesSet() {
		BinParseResultVO resVo = new BinParseResultVO();
		String slaveId = null;
		try {
			slaveId = ProFileUtil.findMsgString(sysconfigFileClasspath,
					"bootstrap.mysql.master.slaveid");
			this.initTarget();
			this.contiParsebinlog(resVo, slaveId);
			boolean isNeedWait = false;
			while (c.isConnected()) {
				BinlogParserManager.getVOFromSession(resVo, new Long(slaveId),
						isNeedWait);
				if (resVo.getEventVOList().size() > 0) {
					this.save2File(resVo);
					BinlogParserManager
							.saveCheckPoint(resVo, new Long(slaveId));
					resVo.setEventVOList(new ArrayList<EventVO>());
				} else {
					Thread.sleep(1000);
					System.out.println("------------休息1000毫秒");
				}
			}
		} catch (Throwable e) {
			String err = e.getMessage();
			err = "解析binlog线程停止，原因:" + err;
			Alarm.sendAlarmEmail(sysconfigFileClasspath, err, resVo.toString());
			e.printStackTrace();
		} finally {
			c.disconnect();
			BinlogParserManager.sessionMap.remove(slaveId);
		}
	}

	private void save2File(BinParseResultVO resVo) throws Exception {
		if (resVo.getEventVOList().size() == 0) {
			return;
		}
		String absDirPath = ProFileUtil.findMsgString(sysconfigFileClasspath,
				"bootstrap.mysql.vo.filepool.dir");
		absDirPath = absDirPath + "/" + serverhost;
		//
		String binfilename_end = resVo.getBinlogfilenameNext();
		Long pos_end = resVo.getBinlogPositionNext();
		String fileNameFullPath = absDirPath + "/" + binfilename_end + "_"
				+ pos_end;
		File target = new File(fileNameFullPath);
		FileUtils.deleteQuietly(target);
		FileUtils.touch(target);
		logger.info(resVo);
		JaxbContextUtil.marshall(resVo, fileNameFullPath);
		ProFileUtil.checkIsExist(fileNameFullPath, true);

	}

	private void initTarget() throws Exception {
		serverhost = ProFileUtil.findMsgString(sysconfigFileClasspath,
				"bootstrap.mysql.master.ip");
		serverPort = ProFileUtil.findMsgString(sysconfigFileClasspath,
				"bootstrap.mysql.master.port");
		username = ProFileUtil.findMsgString(sysconfigFileClasspath,
				"bootstrap.mysql.master.user");
		password = ProFileUtil.findMsgString(sysconfigFileClasspath,
				"bootstrap.mysql.master.pass");
	}

	private void contiParsebinlog(BinParseResultVO resVo, String slaveId)
			throws Throwable {
		String binlogfilename = ProFileUtil.findMsgString(
				checkpointFileClasspath, "192.168.215.1.filename");
		String binlogPosition = ProFileUtil.findMsgString(
				checkpointFileClasspath, "192.168.215.1.pos");
		c = new MysqlConnector(serverhost, new Integer(serverPort), username,
				password);
		BinlogParserManager.startDumpToSession(new Long(slaveId),
				binlogfilename, binlogPosition, c, bps, resVo);

	}

	public static void main(String[] args) {
		try {
			BinlogPBootStrap bs = new BinlogPBootStrap();
			bs.afterPropertiesSet();
			System.out.println("-----------OVER---------------");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void destroy() throws Exception {
		c.disconnect();
	}

}
