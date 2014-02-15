package cn.ce.binlog.mysql.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mysql.jdbc.ResultSetMetaData;

public class TableInfoQuery {
	private final static Log logger = LogFactory.getLog(TableInfoQuery.class);

	public static void query(String user, String password, String ip, int port,
			String dbname, String tableName) throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// 第一步：加载MySQL的JDBC的驱动
			Class.forName("com.mysql.jdbc.Driver");
			// 取得连接的 url,能访问MySQL数据库的用户名,密码；数据库名
			String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbname;
			// 第二步：创建与MySQL数据库的连接类的实例
			conn = DriverManager.getConnection(url, user, password);
			// 第三步：用conn创建Statement对象类实例 stmt
			stmt = conn.createStatement();
			// 第四步：执行查询，用ResultSet类的对象，返回查询的结果
			String sql = "select * from age";
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				logger.info(rs.getString("id")); // 取得数据库中的数据
				logger.info(rs.getString("desc"));
			}
		} catch (ClassNotFoundException e) {
			// 加载JDBC错误,所要用的驱动没有找到
			logger.info("驱动加载错误");
		} catch (SQLException ex) {
			// 显示数据库连接错误或查询错误
			System.err.println("SQLException:" + ex.getMessage());
		} finally {
			try {
				if (rs != null) {
					rs.close();
					rs = null;
				}
				if (stmt != null) {
					stmt.close();
					stmt = null;
				}
				if (conn != null) {
					conn.close();
					conn = null;
				}
			} catch (SQLException e) {
				System.err.println("SQLException:" + e.getMessage());
			}
		}
	}

	public static void queryColumMeta(String user, String password, String ip,
			int port, String dbname, String tablename) throws Exception {

		String jdbcDriver = "com.mysql.jdbc.Driver";
		String jdbcUrl = "jdbc:mysql://" + ip + ":" + port + "/" + dbname;
		Connection conn = null;
		Class.forName(jdbcDriver);
		conn = DriverManager.getConnection(jdbcUrl, user, password);
		PreparedStatement pst = null;
		try {
			String sql = "select * from " + tablename + " where 1=2";
			pst = conn.prepareStatement(sql);
			ResultSetMetaData rsd = (ResultSetMetaData) pst.executeQuery()
					.getMetaData();
			for (int i = 0; i < rsd.getColumnCount(); i++) {
				logger.info("num=" + i);

				logger.info("java类型：" + rsd.getColumnClassName(i + 1));
				logger.info("  数据库类型:" + rsd.getColumnTypeName(i + 1));
				logger.info("  字段名称:" + rsd.getColumnName(i + 1));
				logger.info("  字段长度:" + rsd.getColumnDisplaySize(i + 1));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}

	}

	public static void main(String[] args) {
		try {
			TableInfoQuery.queryColumMeta("root", "qwertyuiop", "localhost",
					3306, "test", "DDD");
			// TableInfoQuery.query("root", "qwertyuiop", "localhost", 3306,
			// "test", "log4j");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
