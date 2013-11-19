package cn.ce.binlog.session;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import cn.ce.binlog.mysql.event.BinlogEvent;
import cn.ce.binlog.mysql.event.FormatDescriptionLogEvent;
import cn.ce.binlog.mysql.event.TableMapLogEvent;
import cn.ce.binlog.mysql.pack.BinlogDumpResPacket;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.query.TableMetaCache;

public class BinlogParseSession {
	private final Map<Long, TableMapLogEvent> mapOfTable = new HashMap<Long, TableMapLogEvent>();
	private FormatDescriptionLogEvent description = FormatDescriptionLogEvent.FORMAT_DESCRIPTION_EVENT_5_x;
	private BinlogPosition logPosition;
	private TableMetaCache tableMetaCache;
	private long slaveId;
	private MysqlConnector c;
//	private final LinkedBlockingQueue<BinlogDumpResPacket> binlogDumpResPacketQueue = new LinkedBlockingQueue<BinlogDumpResPacket>(
//			200);

	private final LinkedBlockingQueue<BinlogEvent> eventVOQueue = new LinkedBlockingQueue<BinlogEvent>(
			200);

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

//	public void addBinlogDumpResPacket(BinlogDumpResPacket binlogDumpResPacket) {
//		binlogDumpResPacketQueue.offer(binlogDumpResPacket);
//	}
//
//	public int getBinlogDumpResPacketQueueSize() {
//		return binlogDumpResPacketQueue.size();
//	}
//
//	public BinlogDumpResPacket getBinlogDumpResPacket()
//			throws InterruptedException {
//		return binlogDumpResPacketQueue.take();
//	}

	public int getEventVOQueueSize() {
		return eventVOQueue.size();
	}

	public void addEventVOQueue(BinlogEvent binlogEvent) {
		eventVOQueue.offer(binlogEvent);
	}

	public BinlogEvent getEventVOQueue() throws InterruptedException {
		return eventVOQueue.take();
	}

	public FormatDescriptionLogEvent getDescription() {
		return description;
	}

	public void setDescription(FormatDescriptionLogEvent description) {
		this.description = description;
	}

	public final void putTable(TableMapLogEvent mapEvent) {
		mapOfTable.put(Long.valueOf(mapEvent.getTableId()), mapEvent);
	}

	public final TableMapLogEvent getTable(final long tableId) {
		return mapOfTable.get(Long.valueOf(tableId));
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

	public long getSlaveId() {
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

}
