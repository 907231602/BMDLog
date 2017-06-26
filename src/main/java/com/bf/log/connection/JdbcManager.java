package com.bf.log.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

public class JdbcManager {
	public static Connection getConnection(Configuration conf) {
		// TODO Auto-generated method stub
		Connection con=null;
		try {
			
			Class.forName(conf.get("driver"));
			
			con=DriverManager.getConnection(conf.get("url"), conf.get("username"), conf.get("userpwd"));
			 //con=DriverManager.getConnection("jdbc:mysql://192.168.0.123:3306/bmobile?useSSL=false", "root", "root");
			// con.setAutoCommit(false);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("con=="+con);
		return con;
	}
}
