package cn.ce.binlog.manager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.TableMapLogEvent;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.query.TableMetaCache;
import cn.ce.binlog.vo.TableMapLogEventVO;
import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;

import com.mongodb.DBObject;

public class Context {
	private volatile boolean						prepareStop					= false;
	private String									connectionsPerHost_s;
	private String									threadsAllowedToBlockForConnectionMultiplier_s;
	//oplog所在库名
	private String									monitortb;
	//所有源库数据都往一个库里导
	private String									forcedbname;
	private String									sourceMongoIpCSV;
	private Integer									sourceMongoPort;
	private String									sourceMongoUser;
	private String									sourceMongoPass;

	private String									descMongoIpCSV;
	private Integer									descMongoPort;
	private String									descMongoUser;
	private String									descMongoPass;

	private MysqlConnector							c;
	private String									serverhost;
	private Integer									serverPort;
	private String									username;
	private String									password;
	private Long									slaveId;
	private String									binlogfilename;
	private Long									binlogPosition;
	private String									binlogcheckfile;

	private Integer									oplogtsInt;
	private Integer									oplogincInt;
	private String									oplogcheckfile;

	private String									zkConInfo;

	private Thread									parseThread;
	private Thread									consumerThread;
	private volatile boolean						parseThreadStop				= false;
	private volatile boolean						consumerThreadStop			= false;

	private volatile boolean						isConsuInSleep				= false;

	// 删除是否为标记删除
	private boolean									isMarkDelete				= false;

	private final int								binQueueSize				= 10000;
	private final int								opQueueSize					= 5000;

	private final LinkedBlockingQueue<BinlogEvent>	eventVOQueue				= new LinkedBlockingQueue<BinlogEvent>(binQueueSize * 2);

	private final LinkedBlockingQueue<DBObject>		dbObjectQueue				= new LinkedBlockingQueue<DBObject>(opQueueSize * 2);
	// id和名字映射
	private TableMapLogEventVO						consumeTableMapLogEventVO;
	private String									consumeTableMapLogEventVOFileCacheString;

	private final Map<Long, TableMapLogEvent>		mapOfTable					= new HashMap<Long, TableMapLogEvent>();
	private Map<Long, String>						tableEventSeriFullPathMap	= new HashMap<Long, String>();

	// desc 而来
	private TableMetaCache							tableMetaCache;

	public final void putTable(TableMapLogEvent mapEvent) throws Exception {
		Long tableId = mapEvent.getTableId();
		mapOfTable.put(Long.valueOf(tableId), mapEvent);
		String serFullPath = this.getTableMapLogEventSeriFullName(tableId);
		BeanUtil.seriObject2File(serFullPath, mapEvent);

	}

	public final TableMapLogEvent getTable(final long tableId) throws Exception {
		TableMapLogEvent mapEvent = mapOfTable.get(Long.valueOf(tableId));
		if (mapEvent == null) {
			String serFullPath = this.getTableMapLogEventSeriFullName(tableId);
			mapEvent = (TableMapLogEvent) BeanUtil.getSeriObjFromFile(serFullPath);
		}
		return mapEvent;
	}

	public final void clearAllTables() {
		mapOfTable.clear();
	}

	public void reset() {
		mapOfTable.clear();
	}

	// parse use
	private String getTableMapLogEventSeriFullName(Long tableId) throws Exception {
		if (!tableEventSeriFullPathMap.containsKey(tableId)) {
			String dir = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "binlogpares.eventseri.dir");
			String tableEventSeriFullPath = dir + "/" + serverhost + "_" + slaveId + "_TableMapLogEvent_" + tableId;
			tableEventSeriFullPathMap.put(tableId, tableEventSeriFullPath);
		}
		String tableEventSeriFullPath = tableEventSeriFullPathMap.get(tableId);
		return tableEventSeriFullPath;

	}

	// consume use
	public String getConsumeTMapLEVOFileCacheString() throws Exception {
		if (StringUtils.isBlank(this.consumeTableMapLogEventVOFileCacheString)) {
			String fileName = "consumeTableMapLogEventVO.cache";
			String serverhost = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mysql.master.ip");
			String slaveId = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.slaveid");
			String dir = ProFileUtil.findMsgString(Const.sysconfigFileClasspath, "bootstrap.mysql.vo.filepool.dir");
			String fileFullPath = dir + "/" + serverhost + "_" + slaveId + "_" + fileName;
			this.consumeTableMapLogEventVOFileCacheString = fileFullPath;
		}
		return this.consumeTableMapLogEventVOFileCacheString;
	}

	public int getEventVOQueueSize() {
		return eventVOQueue.size();
	}

	public void addEventVOQueue(BinlogEvent binlogEvent) throws Exception {
		if (!c.isConnected()) {
			throw new RuntimeException("MySQL Master 连接中断,c:" + c);
		}
		int size = this.getEventVOQueueSize();
		if (size > binQueueSize) {
			Thread.sleep(5);
		}
		eventVOQueue.put(binlogEvent);
	}

	public BinlogEvent getEventVOQueue() throws InterruptedException {
		if (!c.isConnected()) {
			throw new RuntimeException("MySQL Master 连接中断,c:" + c);
		}
		return eventVOQueue.take();
	}

	// -------
	public int getDbObjectQueueSize() {
		return dbObjectQueue.size();
	}

	public void addDbObjectQueue(DBObject dbObject) throws Exception {
		int size = this.getDbObjectQueueSize();
		if (size > opQueueSize) {
			Thread.sleep(5);
		}
		dbObjectQueue.put(dbObject);
	}

	public DBObject getDbObjectQueue() throws InterruptedException {
		return dbObjectQueue.take();
	}

	public Integer getOplogtsInt() {
		return oplogtsInt;
	}

	public void setOplogtsInt(Integer oplogtsInt) {
		this.oplogtsInt = oplogtsInt;
	}

	public Integer getOplogincInt() {
		return oplogincInt;
	}

	public void setOplogincInt(Integer oplogincInt) {
		this.oplogincInt = oplogincInt;
	}

	public String getServerhost() {
		return serverhost;
	}

	public void setServerhost(String serverhost) {
		this.serverhost = serverhost;
	}

	public Integer getServerPort() {
		return serverPort;
	}

	public void setServerPort(Integer serverPort) {
		this.serverPort = serverPort;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Long getSlaveId() {
		return slaveId;
	}

	public void setSlaveId(Long slaveId) {
		this.slaveId = slaveId;
	}

	public String getBinlogfilename() {
		return binlogfilename;
	}

	public void setBinlogfilename(String binlogfilename) {
		this.binlogfilename = binlogfilename;
	}

	public Long getBinlogPosition() {
		return binlogPosition;
	}

	public void setBinlogPosition(Long binlogPosition) {
		this.binlogPosition = binlogPosition;
	}

	public MysqlConnector getC() {
		return c;
	}

	public void setC(MysqlConnector c) {
		this.c = c;
	}

	public boolean isPrepareStop() {
		return prepareStop;
	}

	public void setPrepareStop(boolean prepareStop) {
		this.prepareStop = prepareStop;
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

	public boolean isParseThreadStop() {
		return parseThreadStop;
	}

	public void setParseThreadStop(boolean parseThreadStop) {
		this.parseThreadStop = parseThreadStop;
	}

	public boolean isConsumerThreadStop() {
		return consumerThreadStop;
	}

	public void setConsumerThreadStop(boolean consumerThreadStop) {
		this.consumerThreadStop = consumerThreadStop;
	}

	public TableMetaCache getTableMetaCache() {
		return tableMetaCache;
	}

	public void setTableMetaCache(TableMetaCache tableMetaCache) {
		this.tableMetaCache = tableMetaCache;
	}

	public boolean isConsuInSleep() {
		return isConsuInSleep;
	}

	public void setConsuInSleep(boolean isConsuInSleep) {
		this.isConsuInSleep = isConsuInSleep;
	}

	public TableMapLogEventVO getConsumeTableMapLogEventVO() throws Exception {
		if (this.consumeTableMapLogEventVO == null) {
			String fileFullPath = this.getConsumeTMapLEVOFileCacheString();
			consumeTableMapLogEventVO = BeanUtil.getSeriObjFromFile(fileFullPath);
		}
		return consumeTableMapLogEventVO;
	}

	public String getZkConInfo() {
		return zkConInfo;
	}

	public void setZkConInfo(String zkConInfo) {
		this.zkConInfo = zkConInfo;
	}

	public void setConsumeTableMapLogEventVO(TableMapLogEventVO consumeTableMapLogEventVO) throws Exception {
		this.consumeTableMapLogEventVO = consumeTableMapLogEventVO;
		String fileFullPath = this.getConsumeTMapLEVOFileCacheString();
		File cacheFile = new File(fileFullPath);
		if (!cacheFile.exists()) {
			BeanUtil.seriObject2File(fileFullPath, consumeTableMapLogEventVO);
			ProFileUtil.checkIsExist(fileFullPath, true);
		}
	}

	public boolean isMarkDelete() {
		return isMarkDelete;
	}

	public void setMarkDelete(boolean isMarkDelete) {
		this.isMarkDelete = isMarkDelete;
	}

	public String getSourceMongoIpCSV() {
		return sourceMongoIpCSV;
	}

	public void setSourceMongoIpCSV(String sourceMongoIpCSV) {
		this.sourceMongoIpCSV = sourceMongoIpCSV;
	}

	public Integer getSourceMongoPort() {
		return sourceMongoPort;
	}

	public void setSourceMongoPort(Integer sourceMongoPort) {
		this.sourceMongoPort = sourceMongoPort;
	}

	public String getSourceMongoUser() {
		return sourceMongoUser;
	}

	public void setSourceMongoUser(String sourceMongoUser) {
		this.sourceMongoUser = sourceMongoUser;
	}

	public String getSourceMongoPass() {
		return sourceMongoPass;
	}

	public void setSourceMongoPass(String sourceMongoPass) {
		this.sourceMongoPass = sourceMongoPass;
	}

	public String getDescMongoIpCSV() {
		return descMongoIpCSV;
	}

	public void setDescMongoIpCSV(String descMongoIpCSV) {
		this.descMongoIpCSV = descMongoIpCSV;
	}

	public Integer getDescMongoPort() {
		return descMongoPort;
	}

	public void setDescMongoPort(Integer descMongoPort) {
		this.descMongoPort = descMongoPort;
	}

	public String getDescMongoUser() {
		return descMongoUser;
	}

	public void setDescMongoUser(String descMongoUser) {
		this.descMongoUser = descMongoUser;
	}

	public String getDescMongoPass() {
		return descMongoPass;
	}

	public void setDescMongoPass(String descMongoPass) {
		this.descMongoPass = descMongoPass;
	}

	public String getBinlogcheckfile() {
		return binlogcheckfile;
	}

	public void setBinlogcheckfile(String binlogcheckfile) {
		this.binlogcheckfile = binlogcheckfile;
	}

	public String getOplogcheckfile() {
		return oplogcheckfile;
	}

	public void setOplogcheckfile(String oplogcheckfile) {
		this.oplogcheckfile = oplogcheckfile;
	}

	public String getConnectionsPerHost_s() {
		return connectionsPerHost_s;
	}

	public void setConnectionsPerHost_s(String connectionsPerHost_s) {
		this.connectionsPerHost_s = connectionsPerHost_s;
	}

	public String getThreadsAllowedToBlockForConnectionMultiplier_s() {
		return threadsAllowedToBlockForConnectionMultiplier_s;
	}

	public void setThreadsAllowedToBlockForConnectionMultiplier_s(String threadsAllowedToBlockForConnectionMultiplier_s) {
		this.threadsAllowedToBlockForConnectionMultiplier_s = threadsAllowedToBlockForConnectionMultiplier_s;
	}

	/**
	 * @return the monitortb
	 */
	public String getMonitortb() {
		return monitortb;
	}

	/**
	 * @param monitortb
	 *            the monitortb to set
	 */
	public void setMonitortb(String monitortb) {
		this.monitortb = monitortb;
	}

	/**
	 * @return the forcedbname
	 */
	public String getForcedbname() {
		return forcedbname;
	}

	/**
	 * @param forcedbname
	 *            the forcedbname to set
	 */
	public void setForcedbname(String forcedbname) {
		this.forcedbname = forcedbname;
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}
