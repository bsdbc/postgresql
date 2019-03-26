package com.wwr.util;

import java.io.FileInputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class JDBCUtil {
	private static final Logger logger=Logger.getLogger(JDBCUtil.class);
	private final static ThreadLocal<Connection> mysqlTl=new ThreadLocal<Connection>();
	private final static ThreadLocal<Connection> pgsqlTl=new ThreadLocal<Connection>();
	private static  Properties jdbcProp=null;
	public static final String MYSQL="mysql";
	public static final String PGSQL="pgsql";
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
						logger.debug("获取"+dbType+"连接成功");
					}
				}else if(dbType.equals(PGSQL)){
					conn=pgsqlTl.get();
					if(conn==null){
						url=jdbcProp.getProperty("pgsql.jdbc.url");
						user=jdbcProp.getProperty("pgsql.jdbc.user");
						pwd=jdbcProp.getProperty("pgsql.jdbc.pwd");
						conn=DriverManager.getConnection(url, user, pwd);
						pgsqlTl.set(conn); 
						logger.debug("获取"+dbType+"连接成功");
					}
				}
			} catch (Exception e) {
				logger.error("获取"+dbType+"连接失败",e);
			}
		}
		return conn;
	}
	public static Map<String,String> getFieldsWithMap(String schame,String table) {
		String sql="select col.COLUMN_NAME as columnName,col.COLUMN_TYPE as columnType from debugrmation_schema.`COLUMNS` col where col.TABLE_SCHEMA=? and col.TABLE_NAME=?";
		Connection conn = getConnection(MYSQL);
		if(conn==null){
			logger.error("获取"+schame+"."+table+"表字段失败");
			return null;
		}
		Map<String, String> map = new HashMap<String,String>();
		PreparedStatement ps=null;
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, schame);
			ps.setString(2, table);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				String columnName = rs.getString("columnName");
				String columnType = rs.getString("columnType");
				map.put(columnName, columnType);
			}
			logger.debug("获取"+schame+"."+table+"表字段成功，字段数量："+map.size());
		} catch (Exception e) {
			logger.error("获取"+schame+"."+table+"表字段失败",e);
		}finally{
			try {
				if(ps!=null)
					ps.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return map;
	}
	
	public static String[] getFieldsWithArray(String schame,String table){
		String sql="select col.COLUMN_NAME as columnName from information_schema.`COLUMNS` col where col.TABLE_SCHEMA=? and col.TABLE_NAME=?";
		Connection conn = getConnection(MYSQL);
		if(conn==null){
			logger.error("获取"+schame+"."+table+"表字段失败");
			return null;
		}
		List<String> list=new ArrayList<String>();
		PreparedStatement ps=null;
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, schame);
			ps.setString(2, table);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				String columnName = rs.getString("columnName");
				list.add(columnName);
			}
			logger.debug("获取"+schame+"."+table+"表字段成功，字段数量："+list.size());
		} catch (Exception e) {
			logger.error("获取"+schame+"."+table+"表字段失败",e);
		}finally{
			try {
				if(ps!=null)
					ps.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		String[] array=new String[list.size()];
		array=list.toArray(array);
		return array;
	}
	
	public static List<Map<String,Object>> getData(String schame,String table,int index,int size,String...fields){
		if(fields.length==0){
			fields= getFieldsWithArray(schame, table);
		}
		List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
		StringBuilder sb=new StringBuilder("select ");
		for (String field : fields) {
			sb.append(field+",");
		}
		String sql = sb.substring(0, sb.length()-1);
		if("detect_info_cpu".equalsIgnoreCase(table)){
			sql=sql+" from "+schame+"."+table+" where id>207514396 limit ?,?";
		}else{
			sql=sql+" from "+schame+"."+table+" limit ?,?";
		}
		logger.debug("查询数据SQL-->"+sql);
		Connection conn = getConnection(MYSQL);
		PreparedStatement ps =null;
		try {
			ps= conn.prepareStatement(sql);
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
			logger.debug("获取数据成功,数据量："+list.size());
		} catch (Exception e) {
			logger.error("获取数据失败",e);
		}finally{
			try {
				if(ps!=null)
					ps.close();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
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
		logger.debug("获取数据成功,总数据量："+list.size());
		return list;
	}
	
	public static int getCount(String table) {
		String sql="select count(1) from "+table+" limit 1";
		Connection conn = getConnection(MYSQL);
		int count=0;
		PreparedStatement ps=null;
		try {
			ps= conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				count=rs.getInt(1);
			}
			logger.debug(table+"共有"+count+"条记录");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(ps !=null)
					ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}
	public  static void insertToPgsql(String table,List<Map<String,Object>> datas,String...fields){
		long startTime=System.currentTimeMillis();
		logger.info("开始入库,入库时间："+startTime);
		StringBuffer preSql=new StringBuffer();
		StringBuffer afterSql=new StringBuffer();
		
		preSql.append("insert into "+table+"(");
		afterSql.append(" values(");
		for (String field : fields) {
			preSql.append("\""+field+"\",");
			afterSql.append("?,");
		}
		preSql.deleteCharAt(preSql.length()-1).append(")");
		afterSql.deleteCharAt(afterSql.length()-1).append(")");
		String sql=preSql.toString()+afterSql.toString();
		PreparedStatement ps=null;
		try {
			Connection conn = getConnection(PGSQL);
			ps = conn.prepareStatement(sql);
			for(Map<String,Object> data:datas){
				for (int i=0;i<fields.length;i++) {
					ps.setObject(i+1, data.get(fields[i]));
				}
				ps.addBatch();
			}
			ps.executeBatch();
			long endTime=System.currentTimeMillis();
			logger.info("入库完成时间："+endTime);
			logger.info(table+"插入"+datas.size()+"条数据，耗时"+(endTime-startTime)+"ms");
			if(Constants.WRITEREDIS==1){
				long solt=System.currentTimeMillis();
				String key=table+"::"+solt;
				Jedis jedis = RedisUtil.getJedis();
				jedis.hset(key, "unit", datas.size()+"");
				jedis.hset(key,"time",(endTime-startTime)+"");
				RedisUtil.close(jedis);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			try {
				if(ps!=null)
					ps.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static void main(String[] args) {
		
	}
	public static List<String> getTableNames(String schame) {
		Connection conn = getConnection(MYSQL);
		List<String> list=new ArrayList<String>();
		String sql="select t.TABLE_NAME from  information_schema.`TABLES` t where t.TABLE_SCHEMA='nms' and t.TABLE_TYPE='BASE TABLE'";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while(rs.next()){
				String tableName = rs.getString("TABLE_NAME");
				list.add(tableName);
			}
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		return list;
	}
}
