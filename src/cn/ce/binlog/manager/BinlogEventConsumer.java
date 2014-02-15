package cn.ce.binlog.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.MongoException;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.vo.BinParseResultVO;
import cn.ce.binlog.vo.EventVO;
import cn.ce.binlog.vo.TransCommandVO;
import cn.ce.cons.Const;
import cn.ce.utils.common.ProFileUtil;
import cn.ce.utils.mail.Alarm;

public class BinlogEventConsumer extends AbsDataConsumer {

	private final static Log logger = LogFactory
			.getLog(BinlogEventConsumer.class);

	public void consume(final Context context, final ProcessInfo processInfo,
			Object[] params) {
		MysqlConnector c = context.getC();
		try {
			while (c.isConnected() && !context.isPrepareStop()) {
				try {
					this.consumeFrame(context, processInfo, params);
				} catch (MongoException.Network ex) {
					String msg = ex.getMessage();
					System.err.println("目标Mongodb连接断开，准备重新连接，err:" + msg);
				} catch (InterruptedException ex) {
					if (context.isParseThreadStop()) {
						return;
					}
				}
			}
		} catch (Throwable e) {
			String err = e.getMessage();
			e.printStackTrace();
			err = "xml binlog文件持久化线程停止，原因:" + err;
			Alarm.sendAlarmEmail(Const.sysconfigFileClasspath, err, err + "\n"
					+ context.toString() + "\n");
		} finally {
			context.setPrepareStop(true);
			context.setConsumerThreadStop(true);
			logger.info("---------MySQLEventConsumer持久化文件线程结束!!----------------");
		}
	}

	@Override
	protected TransCommandVO getTransCommandVO(Object rv, Context context)
			throws Exception {
		BinParseResultVO resVo = (BinParseResultVO) rv;
		TransCommandVO tcvo = new TransCommandVO(resVo, context);
		return tcvo;
	}

	@Override
	protected Object event2vo(Context context) throws Exception {
		int len = context.getEventVOQueueSize();
		BinParseResultVO resVo = new BinParseResultVO();
		for (int i = 1; i <= len; i++) {
			BinlogEvent e = context.getEventVOQueue();
			EventVO vo = e.genEventVo();
			if (i == 1) {
				resVo.setBinlogfilenameBegin(e.getHeader().getBinlogfilename());
				resVo.setBinlogPositionBegin(e.getLogPos() == 0 ? 4 : e
						.getLogPos());
			}
			resVo.addEventVOList(vo);
			String binfn = e.getHeader().getBinlogfilename();
			if (!StringUtils.isBlank(binfn)) {
				resVo.setBinlogfilenameNext(binfn);
			}
			if (e.getLogPos() >= 4L) {
				Long pos = e.getLogPos();
				resVo.setBinlogPositionNext(pos);
			}
		}
		return resVo;
	}

	@Override
	protected void saveCheckPoint(Object rv, Context context) throws Exception {
		BinParseResultVO resVo = (BinParseResultVO) rv;
		Long slaveId = context.getSlaveId();
		String posFileAbspath = context.getBinlogcheckfile();
		String filenameKey = slaveId + ".filenameKey";
		String binlogPositionKey = slaveId + ".binlogPosition";
		String binlogfileName = resVo.getBinlogfilenameNext();
		String pos = resVo.getBinlogPositionNext().toString();
		Map<String, String> keyvalue = new HashMap<String, String>();
		keyvalue.put(filenameKey, binlogfileName);
		keyvalue.put(binlogPositionKey, pos);
		if (!StringUtils.isBlank(binlogfileName) && !StringUtils.isBlank(pos)) {
			ProFileUtil.modifyPropertieWithOutFileLock(posFileAbspath,
					keyvalue, false, false);
		}
		resVo.setEventVOList(new ArrayList<EventVO>());

	}

	@Override
	protected int getQueueSize(Context context) throws Exception {
		return context.getEventVOQueueSize();
	}
}
