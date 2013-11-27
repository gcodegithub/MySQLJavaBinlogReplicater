package cn.ce.binlog.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCTest {

	public static void update(String newValue) throws Exception {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://192.168.215.1:3306/test", "canal", "canal");
			conn.setAutoCommit(false);
			PreparedStatement stat = conn
					.prepareStatement("update log4j set CODE=?");
			Long startTime = System.currentTimeMillis();
			stat.setObject(1, newValue);
			stat.executeUpdate();
			conn.commit();
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime);
		} finally {
			if (conn != null)
				conn.close();
		}
	}

	public static void insert(int start, int num) throws Exception {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://192.168.215.1:3306/test", "canal", "canal");
			conn.setAutoCommit(false);
			PreparedStatement stat = conn
					.prepareStatement("insert into log4j values(?,?,?,?)");
			Long startTime = System.currentTimeMillis();
			for (int i = start; i < num; i++) {
				stat.setObject(1, System.currentTimeMillis() + i);
				stat.setObject(2, i);
				stat.setObject(3, i);
				stat.setObject(4, i);
				stat.addBatch();
			}
			stat.executeBatch();
			stat.clearBatch();
			conn.commit();
			long endTime = System.currentTimeMillis();
			System.out.println(endTime - startTime);
		} finally {
			if (conn != null)
				conn.close();
		}
	}

	public static void main(String[] args) {
		try {
			JDBCTest.insert(0, 10000);
			JDBCTest.update("update" + System.currentTimeMillis());
			System.out.println("----------OVER---------------");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
