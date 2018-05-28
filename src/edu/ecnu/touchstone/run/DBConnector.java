package edu.ecnu.touchstone.run;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnector {

	public static Connection getDBConnection(String ip, String port, String dbName, 
			String userName, String passwd) {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://" + ip + ":" + port + "/" + dbName;
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, userName, passwd);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
}
