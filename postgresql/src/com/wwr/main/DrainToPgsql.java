package com.wwr.main;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.wwr.util.JDBCUtil;
import com.wwr.util.Pipe;

public class DrainToPgsql implements Runnable {
	private Logger logger=Logger.getLogger(DrainToPgsql.class);
	private String schame;
	private String table;
	private Pipe<List<Map<String, Object>>> pipe;
	AtomicInteger count;
	public DrainToPgsql(String schame,String table,Pipe<List<Map<String, Object>>> pipe,AtomicInteger count){
		this.schame=schame;
		this.table=table;
		this.pipe=pipe;
		this.count=count;
		count.incrementAndGet();
	}
	@Override
	public void  run() {
		
		while(pipe.getSize()>0){
			String[] fields = JDBCUtil.getFieldsWithArray(schame, table);
			List<Map<String,Object>> pop = pipe.pop();
			if(pop!=null){
				JDBCUtil.insertToPgsql(table, pop, fields);
			}
			
		}
		count.decrementAndGet();
	}
	
}
