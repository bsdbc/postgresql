package com.wwr.main;

import java.util.List;
import java.util.Map;

import com.wwr.util.Constants;
import com.wwr.util.JDBCUtil;
import com.wwr.util.Pipe;
import com.wwr.util.RedisUtil;

public class SourceData implements Runnable {
	private String schame;
	private String table;
	private Pipe<List<Map<String, Object>>> pipe;
	public SourceData(String schame,String table,Pipe<List<Map<String, Object>>> pipe){
		this.schame=schame;
		this.table=table;
		this.pipe=pipe;
	}
	@Override
	public void run() {
		String[] fields = JDBCUtil.getFieldsWithArray(schame, table);
		int total = JDBCUtil.getCount(table);
		int cycle=total%Constants.DATA_PACKAGE_SIZE>0?(total/Constants.DATA_PACKAGE_SIZE+1):(total/Constants.DATA_PACKAGE_SIZE);
		int count=0;
		while(count<=cycle){
			if(pipe.getSize()>=10){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}else{
				List<Map<String, Object>> list = JDBCUtil.getData(schame,table,count*Constants.DATA_PACKAGE_SIZE,Constants.DATA_PACKAGE_SIZE,fields);
				try {
					if(list!=null&&list.size()>0){
						pipe.push(list);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				count++;
			}
			
			
		}
		RedisUtil.getJedis().set(table+"::complate", "true");
	}

}
