package cn.ce.binlog.session;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.FormatDescriptionLogEvent;
import cn.ce.binlog.mysql.event.TableMapLogEvent;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;

public class BinlogParseSession {
	private final Map<Long, TableMapLogEvent> mapOfTable = new HashMap<Long, TableMapLogEvent>();
	private FormatDescriptionLogEvent description = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
	private BinlogPosition logPosition;
	private TableMetaCache tableMetaCache;
	private long slaveId;
	private MysqlConnector c;
	private Thread parseThread;
	private Thread consumerThread;
	private Map<Long, String> tableEventSeriFullPathMap = new HashMap<Long, String>();
	private volatile boolean isConsuInSleep = false;
	private final int queueSize = 80;
	private final LinkedBlockingQueue<BinlogEvent> eventVOQueue = new LinkedBlockingQueue<BinlogEvent>(
			queueSize);

	public final BinlogPosition getLogPosition() {
		return logPosition;
	}

	public final void setLogPosition(BinlogPosition logPosition) {
		this.logPosition = logPosition;
	}

	public final void setLogPosition(String binlogfilename, long binlogPosition) {
		BinlogPosition logPosition = new BinlogPosition(binlogfilename,
				binlogPosition);
		this.logPosition = logPosition;
	}

	public int getEventVOQueueSize() {
		return eventVOQueue.size();
	}

	public void addEventVOQueue(BinlogEvent binlogEvent) throws Exception {
		if (!c.isConnected()) {
			throw new RuntimeException("MySQL Master 连接中断,c:" + c);
		}
		Long lastMem = BeanUtil.getLastAvailMem();
		// System.out.println("--------------------可用内存还剩余(M)：" + lastMem / 1024
		// / 1024);
		if (lastMem / 1024 / 1024 < 256) {
			consumerThread.sleep(20);
			this.consumerThread.interrupt();
		}
		// System.out.println("-------isConsuInSleep:" + isConsuInSleep);
		int size = this.getEventVOQueueSize();
//		System.out.println("-------offer--队列中元素个数:" + size + " "
//				+ System.currentTimeMillis());
		if (isConsuInSleep && size > 2) {
			this.consumerThread.interrupt();
		}
		if (this.queueSize - size > 10) {
			eventVOQueue.offer(binlogEvent);
		} else {
			consumerThread.sleep(20);
			eventVOQueue.offer(binlogEvent);
		}

	}

	public BinlogEvent getEventVOQueue() throws InterruptedException {
		if (!c.isConnected()) {
			throw new RuntimeException("MySQL Master 连接中断,c:" + c);
		}
		return eventVOQueue.take();
	}

	public FormatDescriptionLogEvent getDescription() {
		return description;
	}

	public void setDescription(FormatDescriptionLogEvent description) {
		this.description = description;
	}

	public final void putTable(TableMapLogEvent mapEvent) throws Exception {
		Long tableId = mapEvent.getTableId();
		mapOfTable.put(Long.valueOf(tableId), mapEvent);
		String serFullPath = this.getTableMapLogEventSeriFullName(tableId);
		File serFile = new File(serFullPath);
		if (!serFile.exists()) {
			BeanUtil.seriObject2File(serFullPath, mapEvent);
		}
	}

	public final TableMapLogEvent getTable(final long tableId) throws Exception {
		TableMapLogEvent mapEvent = mapOfTable.get(Long.valueOf(tableId));
		if (mapEvent == null) {
			String serFullPath = this.getTableMapLogEventSeriFullName(tableId);
			mapEvent = (TableMapLogEvent) BeanUtil
					.getSeriObjFromFile(serFullPath);
		}
		return mapEvent;
	}

	public final void clearAllTables() {
		mapOfTable.clear();
	}

	public void reset() {
		description = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
		mapOfTable.clear();
	}

	public TableMetaCache getTableMetaCache() {
		return tableMetaCache;
	}

	public void setTableMetaCache(TableMetaCache tableMetaCache) {
		this.tableMetaCache = tableMetaCache;
	}

	public Long getSlaveId() {
		return slaveId;
	}

	public void setSlaveId(long slaveId) {
		this.slaveId = slaveId;
	}

	public MysqlConnector getC() {
		return c;
	}

	public void setC(MysqlConnector c) {
		this.c = c;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}

	public Thread getParseThread() {
		return parseThread;
	}

	public void setParseThread(Thread parseThread) {
		this.parseThread = parseThread;
	}

	public Thread getConsumerThread() {
		return consumerThread;
	}

	public void setConsumerThread(Thread consumerThread) {
		this.consumerThread = consumerThread;
	}

	private String getTableMapLogEventSeriFullName(Long tableId)
			throws Exception {
		if (!tableEventSeriFullPathMap.containsKey(tableId)) {
			String serverhost = this.getC().getServerhost();
			String slaveId = this.getSlaveId().toString();
			String dir = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "binlogpares.eventseri.dir");
			String tableEventSeriFullPath = dir + "/" + serverhost + "_"
					+ slaveId + "_TableMapLogEvent_" + tableId;
			tableEventSeriFullPathMap.put(tableId, tableEventSeriFullPath);
		}
		String tableEventSeriFullPath = tableEventSeriFullPathMap.get(tableId);
		return tableEventSeriFullPath;

	}

	public boolean isConsuInSleep() {
		return isConsuInSleep;
	}

	public void setConsuInSleep(boolean isConsuInSleep) {
		this.isConsuInSleep = isConsuInSleep;
	}

	public static void main(String[] args) {

	}
}
