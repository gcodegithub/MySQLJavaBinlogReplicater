package cn.ce.binlog.mysql.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;

import cn.ce.binlog.mysql.pack.FieldPacket;
import cn.ce.binlog.mysql.pack.ResultSetPacket;
import cn.ce.binlog.mysql.parse.MysqlConnector;
import cn.ce.binlog.mysql.parse.SocketUnexpectedEndException;
import cn.ce.binlog.mysql.query.TableMeta.FieldMeta;
import cn.ce.cons.Const;
import cn.ce.utils.common.BeanUtil;
import cn.ce.utils.common.ProFileUtil;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

public class TableMetaCache {

	public static final String COLUMN_NAME = "COLUMN_NAME";
	public static final String COLUMN_TYPE = "COLUMN_TYPE";
	public static final String IS_NULLABLE = "IS_NULLABLE";
	public static final String COLUMN_KEY = "COLUMN_KEY";
	public static final String COLUMN_DEFAULT = "COLUMN_DEFAULT";
	public static final String EXTRA = "EXTRA";
	private MysqlConnector connection;
	private Long slaveId;
	private Map<String, String> tableMetaSeriFullPathMap = new HashMap<String, String>();
	// 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
	private ConcurrentMap<String, TableMeta> tableMetaCache;

	private TableMetaCache() {

	}

	public TableMetaCache(MysqlConnector con, long slaveId) {
		this.slaveId = slaveId;
		this.connection = con;
		tableMetaCache = new MapMaker()
				.makeComputingMap(new Function<String, TableMeta>() {

					public TableMeta apply(String name) {
						try {
							return getTableMeta0(name);
						} catch (IOException e) {
							e.printStackTrace();
							// 尝试做一次retry操作
							try {
								connection.reconnect();
								return getTableMeta0(name);
							} catch (Exception e1) {
								String err = e1.getMessage();
								e1.printStackTrace();
								throw new RuntimeException(
										"fetch failed by table meta:" + name
												+ " " + err + e1.getMessage(),
										e1);
							}
						} catch (Exception e) {
							String err = e.getMessage();
							e.printStackTrace();
							throw new RuntimeException(
									"fetch failed by table meta:" + name + " "
											+ err, e);
						}
					}

				});

	}

	public TableMeta getTableMeta(String schema, String table) {
		return getTableMeta(schema, table, true);
	}

	public TableMeta getTableMeta(String schema, String table, boolean useCache) {
		if (!useCache) {
			tableMetaCache.remove(getFullName(schema, table));
		}

		return tableMetaCache.get(getFullName(schema, table));
	}

	public void clearTableMeta(String schema, String table) {
		tableMetaCache.remove(getFullName(schema, table));
	}

	public void clearTableMetaWithSchemaName(String schema) {
		// Set<String> removeNames = new HashSet<String>(); //
		// 存一份临时变量，避免在遍历的时候进行删除
		for (String name : tableMetaCache.keySet()) {
			if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
				// removeNames.add(name);
				tableMetaCache.remove(name);
			}
		}

		// for (String name : removeNames) {
		// tables.remove(name);
		// }
	}

	public void clearTableMeta() {
		tableMetaCache.clear();
	}

	private synchronized TableMeta getTableMeta0(String fullname)
			throws Exception {
		String serFullPath = this.getTableMetaSeriFullName(fullname);
		TableMeta res = null;
		String errMsg = null;
		try {
			ResultSetPacket packet = connection.query("desc " + fullname);
			res = new TableMeta(fullname, parserTableMeta(packet));
			BeanUtil.seriObject2File(serFullPath, res);
		} catch (Throwable e) {
			errMsg = e.getMessage();
			e.printStackTrace();
			System.err.println("获取表元信息出错，从文件中抽取，err:" + errMsg);
			File f = new File(serFullPath);
			if (f.exists() && f.canRead()) {
				res = (TableMeta) BeanUtil.getSeriObjFromFile(serFullPath);
			}
		}
		if (res == null) {
			throw new SocketUnexpectedEndException("获取表元信息出错,err:" + errMsg);
		}
		return res;
	}

	private List<FieldMeta> parserTableMeta(ResultSetPacket packet) {
		Map<String, Integer> nameMaps = new HashMap<String, Integer>(6, 1f);
		int index = 0;
		for (FieldPacket fieldPacket : packet.getFieldDescriptors()) {
			nameMaps.put(fieldPacket.getOriginalName(), index++);
		}

		int size = packet.getFieldDescriptors().size();
		int count = packet.getFieldValues().size()
				/ packet.getFieldDescriptors().size();
		List<FieldMeta> result = new ArrayList<FieldMeta>();
		for (int i = 0; i < count; i++) {
			FieldMeta meta = new FieldMeta();
			// 做一个优化，使用String.intern()，共享String对象，减少内存使用
			meta.setColumnName(packet.getFieldValues()
					.get(nameMaps.get(COLUMN_NAME) + i * size).intern());
			meta.setColumnType(packet.getFieldValues().get(
					nameMaps.get(COLUMN_TYPE) + i * size));
			meta.setIsNullable(packet.getFieldValues().get(
					nameMaps.get(IS_NULLABLE) + i * size));
			meta.setIskey(packet.getFieldValues().get(
					nameMaps.get(COLUMN_KEY) + i * size));
			meta.setDefaultValue(packet.getFieldValues().get(
					nameMaps.get(COLUMN_DEFAULT) + i * size));
			meta.setExtra(packet.getFieldValues().get(
					nameMaps.get(EXTRA) + i * size));

			result.add(meta);
		}

		return result;
	}

	private String getFullName(String schema, String table) {
		StringBuilder builder = new StringBuilder();
		return builder.append('`').append(schema).append('`').append('.')
				.append('`').append(table).append('`').toString();
	}

	private String getTableMetaSeriFullName(String fullname) throws Exception {
		if (!tableMetaSeriFullPathMap.containsKey(fullname)) {
			String serverhost = connection.getAddress().getHostName();
			String dir = ProFileUtil.findMsgString(
					Const.sysconfigFileClasspath, "binlogpares.eventseri.dir");
			String tableEventSeriFullPath = dir + "/" + serverhost + "_"
					+ slaveId + "_TableMeta_" + fullname;
			tableMetaSeriFullPathMap.put(fullname, tableEventSeriFullPath);
		}
		String tableEventSeriFullPath = tableMetaSeriFullPathMap.get(fullname);
		return tableEventSeriFullPath;

	}
}
