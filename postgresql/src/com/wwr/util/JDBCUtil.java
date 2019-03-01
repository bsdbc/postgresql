package com.wwr.util;

import java.io.FileInputStream;
import java.net.URL;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;

public class JDBCUtil {
	private static final Logger logger=Logger.getLogger(JDBCUtil.class);
	private final ThreadLocal mysqlTl=new ThreadLocal();
	private final ThreadLocal pgsqlTl=new ThreadLocal();
	private static  Properties jdbcProp=null;
	static{
		try {
			FileInputStream inStream = null;
			URL resource = JDBCUtil.class.getResource("jdbc.properties");
			jdbcProp=new Properties();
			inStream = new FileInputStream(resource.getPath().replace("%20", " "));
			jdbcProp.load(inStream);
			Class.forName(jdbcProp.getProperty("mysql.jdbc.driver"));
			Class.forName(jdbcProp.getProperty("pgsql.jdbc.driver"));
		} catch (Exception e) {
			logger.error("»ñÈ¡Çý¶¯Ê§°Ü",e);
		}
	}
	
//	public static synchronized Connection getConnection(String dbType){
//		Connection conn=null;
//		if(dbType!=null&&"".equals(dbType)){
//			if(dbType.equals("mysql")){
//				
//			}
//			
//		}
//		
//	}
}
