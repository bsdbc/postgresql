package com.wwr.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.swing.plaf.metal.MetalBorders.TableHeaderBorder;

import com.wwr.util.JDBCUtil;

public class WriteComment {
	public static void main(String[] args) throws SQLException {
//		List<HashMap<String,String>> comments = getComments();
//		writeComments(comments);
		createHyper();
	}
	
	public static List<HashMap<String,String>> getComments() throws SQLException{
		Connection conn = JDBCUtil.getConnection("mysql");
		List<HashMap<String,String>> list=new ArrayList<HashMap<String,String>>();
		String sql="select t.TABLE_NAME as tableName,t.COLUMN_NAME as columnName,t.COLUMN_COMMENT as columnComment from information_schema.`COLUMNS` t where t.`TABLE_SCHEMA`='nms' and t.COLUMN_COMMENT is not null and t.COLUMN_COMMENT !='';";
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		while(rs.next()) {
			HashMap<String,String> map = new HashMap<String,String>();
			String tableName = rs.getString("tableName");
			String columnName = rs.getString("columnName");
			String columnComment = rs.getString("columnComment");
			map.put("\""+tableName+"\".\""+columnName+"\"", columnComment);
			list.add(map);
		}
		return list;
	}
	
	public static void writeComments(List<HashMap<String,String>> list) {
		Connection conn = JDBCUtil.getConnection("pgsql");
		if(list!=null&&list.size()>0){
			for (HashMap<String, String> map : list) {
				Set<Entry<String,String>> entrySet = map.entrySet();
				for (Entry<String, String> entry : entrySet) {
					String key=entry.getKey();
					String value=entry.getValue();
					String sql="COMMENT ON COLUMN "+key+" IS '"+value+"'";
					Statement cs=null;
					try {
						cs = conn.createStatement();
						if(cs!=null){
							cs.execute(sql);
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}finally{
						if(cs!=null){
							try {
								cs.close();
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					
					
				}
			}
		}
	}
	
	public static void createHyper(){
		List<String> list=JDBCUtil.getTableNames("nms");
		Connection conn=null;
		conn= JDBCUtil.getConnection(JDBCUtil.PGSQL);
		if(list!=null&&list.size()>0){
			for (String table : list) {
				if(table!=null&&!table.equals("")&&(table.startsWith("detect")||table.startsWith("di_"))){
					Statement cs=null;
					try {
						cs = conn.createStatement();
						String sql="SELECT create_hypertable('"+table+"', 'DATA_CHECK_TIME')";
						cs.execute(sql);
					} catch (Exception e) {
						e.printStackTrace();
					}finally{
						if(cs!=null){
							try {
								cs .close();
							} catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						
						
					}
					
				}
			}
		}
	}
}
