package com.wwr.util;

import java.io.FileInputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class JDBCUtil {
	private static final Logger logger=Logger.getLogger(JDBCUtil.class);
	private final static ThreadLocal<Connection> mysqlTl=new ThreadLocal<Connection>();
	private final static ThreadLocal<Connection> pgsqlTl=new ThreadLocal<Connection>();
	private static  Properties jdbcProp=null;
	private static final String MYSQL="mysql";
	private static final String PGSQL="pgsql";
	private static final int SIZE=500;
	static{
		try {
			FileInputStream inStream = null;
			URL resource = JDBCUtil.class.getResource("/jdbc.properties");
			jdbcProp=new Properties();
			inStream = new FileInputStream(resource.getPath().replace("%20", " "));
			jdbcProp.load(inStream);
			Class.forName(jdbcProp.getProperty("mysql.jdbc.driver"));
			Class.forName(jdbcProp.getProperty("pgsql.jdbc.driver"));
		} catch (Exception e) {
			logger.error("获取驱动失败",e);
		}
	}
	
	public static synchronized Connection getConnection(String dbType){
		Connection conn=null;
		String url="";
		String user="";
		String pwd="";
		if(dbType!=null&&!"".equals(dbType)){
			try {
				if(dbType.equals(MYSQL)){
					conn = mysqlTl.get();
					if(conn==null){
						url=jdbcProp.getProperty("mysql.jdbc.url");
						user=jdbcProp.getProperty("mysql.jdbc.user");
						pwd=jdbcProp.getProperty("mysql.jdbc.pwd");
						conn=DriverManager.getConnection(url, user, pwd);
						mysqlTl.set(conn);
						logger.info("获取"+dbType+"连接成功");
					}else if(dbType.equals(PGSQL)){
						conn=pgsqlTl.get();
						if(conn==null){
							url=jdbcProp.getProperty("pgsql.jdbc.driver");
							user=jdbcProp.getProperty("pgsql.jdbc.user");
							pwd=jdbcProp.getProperty("pgsql.jdbc.pwd");
							conn=DriverManager.getConnection(url, user, pwd);
							pgsqlTl.set(conn); 
							logger.info("获取"+dbType+"连接成功");
						}
					}
				}
			} catch (Exception e) {
				logger.error("获取"+dbType+"连接失败",e);
			}
		}
		return conn;
	}
	public static Map<String,String> getFieldsWithMap(String schame,String table) {
		String sql="select col.COLUMN_NAME as columnName,col.COLUMN_TYPE as columnType from information_schema.`COLUMNS` col where col.TABLE_SCHEMA=? and col.TABLE_NAME=?";
		Connection conn = getConnection(MYSQL);
		if(conn==null){
			logger.error("获取"+schame+"."+table+"表字段失败");
			return null;
		}
		Map<String, String> map = new HashMap<String,String>();
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, schame);
			ps.setString(2, table);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				String columnName = rs.getString("columnName");
				String columnType = rs.getString("columnType");
				map.put(columnName, columnType);
			}
			logger.info("获取"+schame+"."+table+"表字段成功，字段数量："+map.size());
		} catch (Exception e) {
			logger.error("获取"+schame+"."+table+"表字段失败",e);
		}
		return map;
	}
	
	public static List<String> getFieldsWithList(String schame,String table){
		String sql="select col.COLUMN_NAME as columnName from information_schema.`COLUMNS` col where col.TABLE_SCHEMA=? and col.TABLE_NAME=?";
		Connection conn = getConnection(MYSQL);
		if(conn==null){
			logger.error("获取"+schame+"."+table+"表字段失败");
			return null;
		}
		List<String> list=new ArrayList<String>();
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, schame);
			ps.setString(2, table);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				String columnName = rs.getString("columnName");
				list.add(columnName);
			}
			logger.info("获取"+schame+"."+table+"表字段成功，字段数量："+list.size());
		} catch (Exception e) {
			logger.error("获取"+schame+"."+table+"表字段失败",e);
		}
		return list;
	}
	
	public static List<Map<String,Object>> getData(String schame,String table,int index,int size,String...fields){
		if(fields.length==0){
			List<String> fieldList = getFieldsWithList(schame, table);
			fields=new String[fieldList.size()];
			fields= fieldList.toArray(fields);
		}
		List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
		StringBuilder sb=new StringBuilder("select ");
		for (String field : fields) {
			sb.append(field+" ");
		}
		sb.append("from "+schame+"."+table+" limit ?,?");
		Connection conn = getConnection(MYSQL);
		try {
			PreparedStatement ps = conn.prepareStatement(sb.toString());
			ps.setInt(1, index);
			ps.setInt(2, size);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				Map<String,Object> map=new HashMap<String,Object>();
				for (String field : fields) {
					Object object = rs.getObject(field);
					map.put(field, object);
				}
				list.add(map);
			}
			logger.info("获取数据成功,数据量："+list.size());
		} catch (Exception e) {
			logger.info("获取数据失败",e);
		}
		return list;
	}
	
	public static List<Map<String,Object>> getLimitData(String schame,String table,int total,String...fields){
		List<Map<String,Object>> list=null;
		if(total<SIZE){
			list=getData(schame, table, 0, total, fields);
		}else{
			list=new ArrayList<Map<String,Object>>();
			int cycle=total%SIZE>0?(total/SIZE+1):(total/SIZE);
			int count=0;
			while(cycle!=count){
				List<Map<String, Object>> temp = getData(schame,table,count,SIZE,fields);
				list.addAll(temp);
				count++;
			}
		}
		logger.info("获取数据成功,总数据量："+list.size());
		return list;
	}
	
	public  static void insertToPgsql(String table,List<Map<String,Object>> datas,String...fields){
		long startTime=System.currentTimeMillis();
		StringBuffer preSql=new StringBuffer();
		StringBuffer afterSql=new StringBuffer();
		
		preSql.append("insert into "+table+"(");
		afterSql.append("values(");
		for (String field : fields) {
			preSql.append(field+",");
			afterSql.append("?,");
		}
		preSql.replace(preSql.length()-1, preSql.length()-1, ")");
		afterSql.replace(preSql.length()-1, preSql.length()-1, ")");
		String sql=preSql.toString()+afterSql.toString();
		
		try {
			Connection conn = getConnection(PGSQL);
			PreparedStatement ps = conn.prepareStatement(sql);
			for(Map<String,Object> data:datas){
				for (int i=0;i<fields.length;i++) {
					ps.setObject(i+1, data.get(fields[i]));
				}
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long endTime=System.currentTimeMillis();
		logger.info("插入"+datas.size()+"条数据，耗时"+(endTime-startTime)+"ms");
	}
	
	public static void main(String[] args) {
		
	}
}
