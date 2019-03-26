package com.wwr.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.wwr.util.ExcelUtil;
import com.wwr.util.JDBCUtil;
import com.wwr.util.Pipe;
import com.wwr.util.RedisUtil;
import com.wwr.util.ThreadManager;
import com.wwr.util.ThreadManager.ThreadPollProxy;

import redis.clients.jedis.Jedis;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;

public class DataInput {
	public static void main(String[] args) {
		final ThreadPollProxy threadPool = ThreadManager.getThreadPollProxy();
		String[] tables={"detect_info_cpu"};
		for (final String table : tables) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					dataInput(threadPool, table);
				}
			}).start();
			
		}
		
		
	}
//	public static void main(String[] args) {
//		 ThreadPollProxy threadPool = ThreadManager.getThreadPollProxy();
//		String[] tables={"detection_info","detect_info_cpu","di_switchport"};
//		for ( String table : tables) {
//			dataInput(threadPool, table);
//		}
//	}
	private static void dataInput(ThreadPollProxy threadPool, String table) {
		Pipe<List<Map<String, Object>>> pipe = new Pipe<List<Map<String, Object>>>();
		threadPool.execute(new SourceData("nms", table, pipe));
		AtomicInteger count=new AtomicInteger(0);
		int polling=0;
		while(true){
			if(pipe.getSize()>=1&&count.get()<1){
				threadPool.execute(new DrainToPgsql("nms",table, pipe,count));
				continue;
			}else{
				
				Jedis jedis = RedisUtil.getJedis();
				String flag=jedis.get(table+"::complate");
				RedisUtil.close(jedis);;
				if(flag!=null&&!"".equals(flag)&&"true".equals(flag)){
					polling++;
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if(polling>3){//在查询线程将数据全部放入管道的情况下，轮询三次，确认管道中所有的数据都已经入库
				break;
			}
		}
		//执行到这里说明一张表已经完全迁移完成，对结果进行统计
		analysis(table);
	}
	
	public static void analysis(String table){
		Jedis jedis = RedisUtil.getJedis();
		Set<String> keys = jedis.keys(table+"::1*");
		int count=keys.size();
		List<List<String>> rowDatas=new ArrayList<List<String>>();
		
		long total=0L;
		long timeUse=0L;
		double avg=0D;
		for (String key : keys) {
			if("hash".equalsIgnoreCase(jedis.type(key))){
				List<String> rowData=new ArrayList<String>();
				String unit = jedis.hget(key, "unit");
				String time = jedis.hget(key, "time");
				rowData.add(table);
				rowData.add("");
				rowData.add(unit);
				rowData.add(time);
				rowData.add("");
				rowData.add("");
				rowDatas.add(rowData);
				total+=Long.parseLong(unit);
				timeUse+=Long.parseLong(time);
			}
		}
		avg=timeUse/count;
		try {
			File file = new File("C:\\Users\\Administrator.SC-201902220916\\Desktop\\"+table+System.currentTimeMillis()+".xls");
			HSSFWorkbook workbook = new HSSFWorkbook();
			ExcelUtil excelUtil = new ExcelUtil(workbook);
			int sheetIndex = excelUtil.createSheet();
			excelUtil.setSheetName(sheetIndex, table+System.currentTimeMillis());
			List<String> head=new ArrayList<String>();
			head.add("表名");
			head.add("数据总量");
			head.add("批次插入单位");
			head.add("耗时");
			head.add("平均耗时");
			head.add("总耗时");
			excelUtil.setRowValue(sheetIndex, head, 0);
			List<String> countInfo=new ArrayList<String>();
			countInfo.add(table);
			countInfo.add(total+"");
			countInfo.add("");
			countInfo.add("");
			countInfo.add(avg+"");
			countInfo.add(timeUse+"");
			excelUtil.setRowValue(sheetIndex, countInfo, 1);
			FileOutputStream os = new FileOutputStream(file);
			excelUtil.write(sheetIndex, rowDatas, 2, os);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
