package cn.ce.binlog.mongo.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;

import cn.ce.binlog.manager.Context;
import cn.ce.binlog.mysql.event.ColumnInfoValue;
import cn.ce.binlog.vo.SQLRowVO;
import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.mail.Alarm;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public abstract class AbsMongoDao {

	protected DBObject getKeyValueMap(SQLRowVO row) throws Exception {
		DBObject keyvalue = new BasicDBObject();
		for (ColumnInfoValue civ : row.getRowValueInfo()) {
			String cn = civ.getColumnName();
			keyvalue.put(cn, civ.getReadValue());
		}
		return keyvalue;
	}

	public void closeDBCon() {
		MongoConnectionFactory.close();
	}

	protected Object getPrimaryValue(Object r) {
		if (r instanceof SQLRowVO) {
			SQLRowVO row = (SQLRowVO) r;
			Integer index = row.getPrimaryKeyIndex();
			if (index == null) {
				return null;
			}
			ColumnInfoValue pciv = row.getRowValueInfo().get(row.getPrimaryKeyIndex());
			String priKey = pciv.getColumnName();
			Object privalue = pciv.getReadValue();
			return privalue;
		} else if (r instanceof DBObject) {
			DBObject row = (DBObject) r;
			String priKey = "_id";
			Object privalue = row.get(priKey);
			return privalue;
		}

		return null;
	}

	protected String getPrimaryKey(Object r) {
		if (r instanceof SQLRowVO) {
			SQLRowVO row = (SQLRowVO) r;
			Integer index = row.getPrimaryKeyIndex();
			if (index == null) {
				return null;
			}
			ColumnInfoValue pciv = row.getRowValueInfo().get(row.getPrimaryKeyIndex());
			String priKey = pciv.getColumnName();
			return priKey;
		} else if (r instanceof DBObject) {
			DBObject row = (DBObject) r;
			String priKey = "_id";
			return priKey;
		}
		return null;
	}

	protected String getDbName(Object r) {
		if (r instanceof SQLRowVO) {
			SQLRowVO row = (SQLRowVO) r;
			String dbname = row.getDbname();
			return dbname;
		} else if (r instanceof DBObject) {
			DBObject row = (DBObject) r;
			String dbname = row.get("dbname").toString();
			return dbname;
		}
		return null;
	}

	// tbname
	protected String getTbName(Object r) {
		if (r instanceof SQLRowVO) {
			SQLRowVO row = (SQLRowVO) r;
			String tbname = row.getTablename();
			return tbname;
		} else if (r instanceof DBObject) {
			DBObject row = (DBObject) r;
			String tbname = row.get("tbname").toString();
			return tbname;
		}
		return null;
	}

	protected String getDmlType(Object r) {
		if (r instanceof SQLRowVO) {
			SQLRowVO row = (SQLRowVO) r;
			String dmltype = row.getDmlType();
			return dmltype;
		} else if (r instanceof DBObject) {
			DBObject row = (DBObject) r;
			String dmlType = (String) row.get("dmlType");
			return dmlType;
		}
		return null;
	}

	protected DBObject getMongoRow(Object r) throws Exception {
		if (r instanceof SQLRowVO) {
			SQLRowVO row = (SQLRowVO) r;
			DBObject o = this.getKeyValueMap(row);
			return o;
		} else if (r instanceof DBObject) {
			DBObject row = (DBObject) r;
			return row;
		}
		return null;
	}

	public void cutSync2Mongodb(final java.util.ArrayList list, Context ctx, Object[] params) throws Exception {
		boolean isMark = ctx.isMarkDelete();
		List<Object> source = list;
		DBCollection dbc = null;
		Map<String, List<DBObject>> batchIns = new HashMap<String, List<DBObject>>();
		String desc_ipcsv = ctx.getDescMongoIpCSV();
		int port = ctx.getDescMongoPort();
		for (Object row : source) {
			String priKey = this.getPrimaryKey(row);
			Object privalue = this.getPrimaryValue(row);
			if (privalue == null) {
				System.err.println("一行数据主键值为null，忽略掉该数据,row:" + row);
				continue;
			}

			String dbname = this.getDbName(row);
			String tbname = this.getTbName(row);
			String dmlType = this.getDmlType(row);
			dbc = MongoConnectionFactory.getMongoTBConn(desc_ipcsv, port, dbname, tbname);

			//
			MongoConnectionFactory.createIndex(desc_ipcsv, port, dbname, tbname, priKey);
			MongoConnectionFactory.createIndex(desc_ipcsv, port, dbname, tbname, "dvs_trace_code", "dvs_mysql_op_type", "dvs_time_rec", priKey, "_id");
			//
			String batchKey = dbname + "." + tbname;
			DBObject dbo = this.getMongoRow(row);
			dbo.put("dvs_time_rec", System.currentTimeMillis());
			dbo.put("dvs_trace_code", BeanUtil.getRandomInt(0, 100));
			dbo.put("dvs_mysql_op_type", dmlType.toUpperCase());

			DBObject s = new BasicDBObject();
			s.put(priKey, privalue);
			if (Const.DELETE.equalsIgnoreCase(dmlType)) {
				if (isMark) {
					if (batchIns.containsKey(batchKey)) {
						List<DBObject> valueList = batchIns.get(batchKey);
						valueList.add(dbo);
					} else {
						List<DBObject> valueList = new ArrayList<DBObject>(5000);
						batchIns.put(batchKey, valueList);
						valueList.add(dbo);
					}
				} else {
					dbc.remove(s, WriteConcern.SAFE);
				}

			} else if (Const.INSERT.equalsIgnoreCase(dmlType)) {
				if (batchIns.containsKey(batchKey)) {
					List<DBObject> valueList = batchIns.get(batchKey);
					valueList.add(dbo);
				} else {
					List<DBObject> valueList = new ArrayList<DBObject>(5000);
					batchIns.put(batchKey, valueList);
					valueList.add(dbo);
				}
			} else if (Const.UPDATE.equalsIgnoreCase(dmlType)) {
				dbc.update(s, dbo, false, false, WriteConcern.SAFE);
			} else if (Const.UPDATE_PART.equals(dmlType)) {
				dbc.update(s, new BasicDBObject().append("$set", dbo.get("$set")),false,true);
			}

		}// for over
		for (String batchKey : batchIns.keySet()) {
			if (StringUtils.isBlank(batchKey)) {
				continue;
			}
			List<DBObject> valueList = batchIns.get(batchKey);
			String[] splitArray = batchKey.split("\\.");
			String dbnameString = splitArray[0];
			String tbnameString = splitArray[1];
			dbc = MongoConnectionFactory.getMongoTBConn(desc_ipcsv, port, dbnameString, tbnameString);
			dbc.insert(valueList, WriteConcern.SAFE);
		}

	}

	protected void normalSync2Mongodb(final java.util.ArrayList list, Context ctx, Object[] params) throws Exception {
		boolean isMark = ctx.isMarkDelete();
		String desc_ipcsv = ctx.getDescMongoIpCSV();
		int port = ctx.getDescMongoPort();
		List<Object> source = list;
		DBCollection dbc = null;
		for (Object row : source) {
			String priKey = this.getPrimaryKey(row);
			Object privalue = this.getPrimaryValue(row);
			if (privalue == null) {
				System.err.println("一行数据主键值为null，忽略掉该数据,row:" + row);
				continue;
			}
			String dbname = this.getDbName(row);
			String tbname = this.getTbName(row);
			String dmlType = this.getDmlType(row);
			dbc = MongoConnectionFactory.getMongoTBConn(desc_ipcsv, port, dbname, tbname);

			//
			MongoConnectionFactory.createIndex(desc_ipcsv, port, dbname, tbname, priKey);
			MongoConnectionFactory.createIndex(desc_ipcsv, port, dbname, tbname, "dvs_trace_code", "dvs_mysql_op_type", "dvs_time_rec", priKey, "_id");

			//
			DBObject dbo = this.getMongoRow(row);
			dbo.put("dvs_trace_code", BeanUtil.getRandomInt(0, 100));
			dbo.put("dvs_mysql_op_type", dmlType.toUpperCase());
			dbo.put("dvs_time_rec", System.currentTimeMillis());
			DBObject s = new BasicDBObject();
			s.put(priKey, privalue);
			if (Const.DELETE.equalsIgnoreCase(dmlType)) {
				if (isMark) {
					dbc.update(s, dbo, true, false, WriteConcern.SAFE);
				} else {
					dbc.remove(s, WriteConcern.SAFE);
				}
			} else if (Const.UPDATE.equalsIgnoreCase(dmlType) || Const.INSERT.equalsIgnoreCase(dmlType)) {
				dbc.update(s, dbo, true, false, WriteConcern.SAFE);
			} else if (Const.UPDATE_PART.equals(dmlType)) {
				dbc.update(s, new BasicDBObject().append("$set", dbo.get("$set")),true,true);
			}

		}

	}
}
