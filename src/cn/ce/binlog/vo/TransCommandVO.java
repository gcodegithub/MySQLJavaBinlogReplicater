package cn.ce.binlog.vo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.bson.types.ObjectId;

import cn.ce.binlog.manager.Context;
import cn.ce.binlog.mysql.event.ColumnInfoValue;
import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;

import com.mongodb.DBObject;

public class TransCommandVO {

	private static Set<String> includeTB = new HashSet<String>();
	private static Boolean isDoIncludeTB = false;
	private static Set<String> includeDB = new HashSet<String>();
	private static Boolean isDoIncludeDB = false;

	static {
		try {
			String booltb = ProFileUtil.findMsgString(
					Const.filterFileClasspath, "include.db.control");
			if (!StringUtils.isBlank(booltb)) {
				isDoIncludeTB = Boolean.valueOf(booltb);
				String csvtb = ProFileUtil.findMsgString(
						Const.filterFileClasspath, "include.tb");
				List<String> tblist = BeanUtil.csvToList(csvtb.toLowerCase(),
						",");
				includeTB.addAll(tblist);
			}
			String booldb = ProFileUtil.findMsgString(
					Const.filterFileClasspath, "include.db.control");
			if (!StringUtils.isBlank(booldb)) {
				isDoIncludeDB = Boolean.valueOf(booldb);
				String csvdb = ProFileUtil.findMsgString(
						Const.filterFileClasspath, "include.db");
				List<String> dblist = BeanUtil.csvToList(csvdb.toLowerCase(),
						",");
				includeDB.addAll(dblist);
			}
			// System.out.println(includeTB);
			// System.out.println(isDoIncludeTB);
			// System.out.println(includeDB);
			// System.out.println(isDoIncludeDB);
		} catch (Exception e) {
			String msg = e.getMessage();
			e.printStackTrace();
			throw new RuntimeException(msg);
		}
	}

	private String temfilepath;
	private Long logPos;
	private String binlogFileName;
	private Map<String, List<Object>> rowVoListMap = new HashMap<String, List<Object>>();

	public Map<String, List<Object>> getRowVoListMap() {
		return rowVoListMap;
	}

	public Long getLogPos() {
		return logPos;
	}

	public void setLogPos(Long logPos) {
		this.logPos = logPos;
	}

	public String getBinlogFileName() {
		return binlogFileName;
	}

	public void setBinlogFileName(String binlogFileName) {
		this.binlogFileName = binlogFileName;
	}

	public String getTemfilepath() {
		return temfilepath;
	}

	public void setTemfilepath(String temfilepath) {
		this.temfilepath = temfilepath;
	}

	public TransCommandVO(BinParseResultVO bprvo, Context context)
			throws Exception {
		List<EventVO> evoList = bprvo.getEventVOList();
		for (EventVO evo : evoList) {
			this.setBinlogFileName(evo.getBinlogFileName());
			this.setLogPos(evo.getLogPos());
			if (evo instanceof QueryLogEventVO) {

			} else if (evo instanceof TableMapLogEventVO) {
				context.setConsumeTableMapLogEventVO((TableMapLogEventVO) evo);

			} else if (evo instanceof FormatDescriptionLogEventVO) {

			} else if (evo instanceof RotateLogEventVO) {

			} else if (evo instanceof RowEventVO) {
				RowEventVO revo = (RowEventVO) evo;
				int columnLen = revo.getColumnLen();
				String rowDMLType = revo.getRowDMLType();
				long when = revo.getWhen();
				List<ColumnInfoValue> beforeColumnInfo = revo
						.getBeforeColumnInfo();
				List<ColumnInfoValue> afterColumnInfo = revo
						.getAfterColumnInfo();
				this.splitRow(context, columnLen, rowDMLType, when,
						beforeColumnInfo, afterColumnInfo);
			} else if (evo instanceof StopLogEventVO) {

			} else {

			}
		}
	}

	/*
	 * { "ts" : { "t" : 1294582141000, "i" : 11 }, "op" : "i", "ns" :
	 * "mixi_top_city.building_77", "o" : { "_id" : "8577977_215_44_38", "uid" :
	 * "8577977", "x" : 44, "y" : 38, "pos" : 1, "btime" : 1293955498, "ntime" :
	 * 1294486420, "bid" : 18, "extprop" : 0, "status" : 0, "ucid" : 215 } }
	 */
	public TransCommandVO(OpParseResultVO resVo, Context context) {
		List<DBObject> eventVOList = resVo.getEventVOList();
		for (DBObject value : eventVOList) {
			String ns = (String) value.get("ns");
			String[] splitArray = ns.split("\\.");
			String dbname = splitArray[0];
			String tablename = splitArray[1];
			String prikey = "_id";
			ObjectId priValue = (ObjectId) value.get(prikey);
			String key = dbname + "." + tablename + "." + prikey + "="
					+ priValue;
			String optype = value.get("op").toString();
			String dmlType = null;
			if ("i".equals(optype)) {
				dmlType = "INSERT";
			} else if ("u".equals(optype)) {
				if (value.get("$set") != null) {
					dmlType = "UPDATE_PART";
				} else {
					dmlType = "UPDATE";
				}
			} else if ("d".equals(optype)) {
				dmlType = "DELETE";
			}
			value.put("dbname", dbname);
			value.put("dmlType", dmlType);
			value.put("tbname", tablename);
			// filter过滤器
			if (isDoIncludeDB) {
				if (!includeDB.contains(dbname.toLowerCase())) {
					// System.out.println("库不在同步配置中，不进行同步,dbname:" + dbname);
					continue;
				}
			}
			if (isDoIncludeTB) {
				if (!includeTB.contains((dbname + "." + tablename)
						.toLowerCase())) {
					// System.out.println("表不在同步配置中，不进行同步,tablename:" +
					// tablename);
					continue;
				}
			}
			if (rowVoListMap.containsKey(key)) {
				List<Object> list = rowVoListMap.get(key);
				list.add(value);
			} else {
				List<Object> list = new ArrayList<Object>();
				list.add(value);
				rowVoListMap.put(key, list);
			}
		}
	}

	private void splitRow(Context context, int columnLen, String rowDMLType,
			long when, List<ColumnInfoValue> beforeColumnInfo,
			List<ColumnInfoValue> afterColumnInfo) throws Exception {
		TableMapLogEventVO tevo = context.getConsumeTableMapLogEventVO();
		String dbname = tevo.getDbname();
		String tblname = tevo.getTblname();
		this.setBinlogFileName(tevo.getBinfile());
		if ("UPDATE".equals(rowDMLType)) {
			this.columInfo2RowInfo(dbname, rowDMLType, tblname, columnLen,
					when, afterColumnInfo);
		} else if ("INSERT".equals(rowDMLType)) {
			this.columInfo2RowInfo(dbname, rowDMLType, tblname, columnLen,
					when, afterColumnInfo);
		} else if ("DELETE".equals(rowDMLType)) {
			this.columInfo2RowInfo(dbname, rowDMLType, tblname, columnLen,
					when, beforeColumnInfo);
		}
	}

	private void columInfo2RowInfo(String dbname, String dmlType,
			String tablename, int columnLen, long when,List<ColumnInfoValue> cif) {
		int pos = 0;

		for (int j = 0; j < cif.size() / columnLen; j++) {
			SQLRowVO Object = new SQLRowVO();
			Object.setDbname(dbname);
			Object.setTablename(tablename);
			Object.setDmlType(dmlType);
			Object.setWhen(when);
			String priValue = "";
			String prikey = "";
			for (int i = 0; i < columnLen; i++) {
				ColumnInfoValue civ = cif.get(pos);
				Object.addRowValueInfo(civ);
				if (civ.isPrimaryKey()) {
					Object.setPrimaryKeyIndex(i);
					priValue = civ.getValue();
					prikey = civ.getColumnName();
				}
				pos++;
			}
			String key = dbname + "." + tablename + "." + prikey + "="
					+ priValue;
			// filter过滤器
			if (isDoIncludeDB) {
				if (!includeDB.contains(dbname.toLowerCase())) {
					// System.out.println("库不在同步配置中，不进行同步,dbname:" + dbname);
					continue;
				}
			}
			if (isDoIncludeTB) {
				if (!includeTB.contains((dbname + "." + tablename)
						.toLowerCase())) {
					// System.out.println("表不在同步配置中，不进行同步,tablename:" +
					// tablename);
					continue;
				}
			}
			if (rowVoListMap.containsKey(key)) {
				List<Object> list = rowVoListMap.get(key);
				list.add(Object);
			} else {
				List<Object> list = new ArrayList<Object>();
				list.add(Object);
				rowVoListMap.put(key, list);
			}

		}// for over
	}

	@Override
	public String toString() {
		String s = ToStringBuilder.reflectionToString(this,
				ToStringStyle.MULTI_LINE_STYLE);
		return s;
	}
}