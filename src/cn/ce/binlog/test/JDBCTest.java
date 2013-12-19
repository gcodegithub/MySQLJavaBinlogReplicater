package cn.ce.binlog.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCTest {

	public static void update() throws Exception {
		Connection conn = null;
		int i = 0;
		while (true) {
			i++;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				conn = DriverManager.getConnection(
						"jdbc:mysql://192.168.215.1:3306/test", "canal",
						"canal");
				conn.setAutoCommit(false);
				PreparedStatement stat = conn
						.prepareStatement("update log4j set CODE=?");
				Long startTime = System.currentTimeMillis();
				stat.setObject(1, startTime+(i*5));
				stat.executeUpdate();
				conn.commit();
				long endTime = System.currentTimeMillis();
				System.out.println(endTime - startTime);
			} finally {
				if (conn != null)
					conn.close();
			}
			Thread.sleep(500);
		}
	}

	public static void insert() throws Exception {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://192.168.215.1:3306/test", "canal", "canal");
			conn.setAutoCommit(false);
			PreparedStatement stat = conn
					.prepareStatement("insert into log4j values(?,?,?,?)");
			PreparedStatement stat1 = conn
					.prepareStatement("insert into log5j values(?,?,?,?)");

			for (int j = 0; j < 2; j++) {
				for (int i = 0; i < 1000; i++) {
					stat.setObject(1, System.currentTimeMillis() + i);
					stat.setObject(2, i);
					stat.setObject(3, i);
					stat.setObject(4, i);
					stat.addBatch();
					stat1.setObject(1, System.currentTimeMillis() + i);
					stat1.setObject(2, i);
					stat1.setObject(3, i);
					stat1.setObject(4, i);
					stat1.addBatch();
				}
				stat.executeBatch();
				stat.clearBatch();
				stat1.executeBatch();
				stat1.clearBatch();
				conn.commit();
				Thread.sleep(1000);
				System.out.println("----------一轮循环---------------");
			}
		} finally {
			if (conn != null)
				conn.close();
		}
	}

	public static void main(String[] args) {
		try {
			JDBCTest.insert();
//			JDBCTest.update();
			System.out.println("----------OVER---------------");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
