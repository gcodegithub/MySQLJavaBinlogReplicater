package cn.ce.binlog.mongo.simple;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MongoConnectionFactory {
	private final static Log logger = LogFactory
			.getLog(MongoConnectionFactory.class);
	private static Lock lock = new ReentrantLock();
	// private static String username;
	// private static String passwd;
	private final static CopyOnWriteArraySet<String> indexSet = new CopyOnWriteArraySet<String>();
	private static Map<String, DBCollection> tbMap = new ConcurrentHashMap<String, DBCollection>();
	private static Map<String, MongoClient> dbMap = new ConcurrentHashMap<String, MongoClient>();

	// public static Map<String, Object> parseJSON2Map(String jsonStr) {
	// Map<String, Object> map = new HashMap<String, Object>();
	// JSONObject json = JSONObject.fromObject(jsonStr);
	// for (Object k : json.keySet()) {
	// Object v = json.get(k);
	// if (v instanceof JSONArray) {
	// List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
	// Iterator<JSONObject> it = ((JSONArray) v).iterator();
	// while (it.hasNext()) {
	// JSONObject json2 = it.next();
	// list.add(parseJSON2Map(json2.toString()));
	// }
	// map.put(k.toString(), list);
	// } else {
	// map.put(k.toString(), v);
	// }
	// }
	// return map;
	// }

	public static void createIndex(String ipcsv, Integer port, String dbname,
			String tbname, String username, String passwd,
			String connectionsPerHost_s,
			String threadsAllowedToBlockForConnectionMultiplier_s,
			String... columName2Indexs) throws Exception {
		StringBuilder muliKeyName = new StringBuilder();
		BasicDBObject copmIndex = new BasicDBObject();
		for (String oneName : columName2Indexs) {
			muliKeyName.append(oneName);
			muliKeyName.append(".");
			copmIndex.append(oneName, 1);
		}
		muliKeyName.append("dvs");
		String indexKeyName = ipcsv + "." + port + "." + dbname + "." + tbname
				+ "." + muliKeyName.toString();
		//
		if (indexSet.add(indexKeyName)) {
			if (columName2Indexs != null && columName2Indexs.length == 1
					&& "_id".equalsIgnoreCase(columName2Indexs[0])) {
				logger.info("no need create index,columName2Indexs:"
						+ columName2Indexs);
				return;
			}
			DBCollection dbc = MongoConnectionFactory.getMongoTBConn(ipcsv,
					port, dbname, tbname, username, passwd,
					connectionsPerHost_s,
					threadsAllowedToBlockForConnectionMultiplier_s);
			dbc.createIndex(
					copmIndex,
					new BasicDBObject().append("background", true)
							.append("unique", false).append("dropDups", true)
							.append("ns", dbc.getFullName())
							.append("name", muliKeyName.toString()));
			logger.info("create mongodb index>>>>>>>>> indexKeyName:"
					+ indexKeyName + " muliKeyName:" + muliKeyName.toString());
		}
	}

	private static MongoClient getMongoClient(String ipcsv, int port,
			String connectionsPerHost_s,
			String threadsAllowedToBlockForConnectionMultiplier_s) {
		String dbKeyName = ipcsv + "." + port;
		try {
			MongoConnectionFactory.lock.lock();
			if (dbMap.containsKey(dbKeyName)) {
				MongoClient mc = dbMap.get(dbKeyName);
				return mc;
			}
			List<String> ips = BeanUtil.csvToList(ipcsv, ",");
			List<ServerAddress> seeds = new ArrayList<ServerAddress>(5);
			for (String ip : ips) {
				seeds.add(new ServerAddress(ip, port));
			}
			MongoClient mc = new MongoClient(seeds);
			MongoOptions opt = mc.getMongoOptions();
			opt.setConnectionsPerHost(new Integer(connectionsPerHost_s));
			opt.setThreadsAllowedToBlockForConnectionMultiplier(new Integer(
					threadsAllowedToBlockForConnectionMultiplier_s));
			opt.autoConnectRetry = true;
			dbMap.put(dbKeyName, mc);
			return mc;
		} catch (Throwable t) {
			String msg = t.getMessage();
			t.printStackTrace();
			dbMap.remove(dbKeyName);
			throw new RuntimeException(msg);
		} finally {
			MongoConnectionFactory.lock.unlock();
		}
	}

	public static DBCollection getMongoTBConn(String ipcsv, int port,
			String mongodbname, String mongotbname, String username,
			String passwd, String connectionsPerHost_s,
			String threadsAllowedToBlockForConnectionMultiplier_s)
			throws Exception {
		String tbkey = ipcsv + "." + port + "." + mongodbname + "."
				+ mongotbname;
		try {
			MongoConnectionFactory.lock.lock();
			if (tbMap.containsKey(tbkey)) {
				DBCollection tb = tbMap.get(tbkey);
				return tb;
			}
			MongoClient mclinet = MongoConnectionFactory.getMongoClient(ipcsv,
					port, connectionsPerHost_s,
					threadsAllowedToBlockForConnectionMultiplier_s);
			DB db = mclinet.getDB(mongodbname);
			if (!StringUtils.isBlank(username)) {
				boolean auth = db.authenticate(username, passwd.toCharArray());
				if (auth) {
					System.out.println("用户授权通过");
				} else {
					throw new RuntimeException("用户授权不通过,ipcsv=" + ipcsv
							+ " dbname=" + mongodbname + " port=" + port
							+ " username=" + username + " passwd=" + passwd);
				}
			}
			DBCollection col = db.getCollection(mongotbname);
			col.setWriteConcern(WriteConcern.SAFE);
			tbMap.put(tbkey, col);
			return col;
		} catch (Throwable t) {
			String msg = t.getMessage();
			t.printStackTrace();
			tbMap.remove(tbkey);
			throw new RuntimeException(msg);
		} finally {
			MongoConnectionFactory.lock.unlock();
		}

	}

	public static void close() {
		for (String ipcsv : dbMap.keySet()) {
			MongoClient mclinet = dbMap.get(ipcsv);
			if (mclinet != null) {
				mclinet.close();
			}
		}
		dbMap.clear();
		tbMap.clear();
		indexSet.clear();

	}

	public static void main(String[] args) throws UnknownHostException {
		DBCollection dbc;
		try {

			dbc = MongoConnectionFactory.getMongoTBConn("", 27017, "", "",
					"test", "log4j", "", "");
			DBObject dbo = new BasicDBObject();
			dbo.put("key111", "value");
			dbc.insert(dbo, WriteConcern.SAFE);
			dbc.ensureIndex("key111");
			System.out.println("++++++++++++++++");
		} catch (MongoException e) {
			System.out.println("--------------");
			// e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
