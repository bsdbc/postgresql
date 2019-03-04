package com.wwr.util;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DataInput {
	public static void main(String[] args) {
		Pipe<Map<String, Object>> pipe = new Pipe<Map<String,Object>>();
		
		List<String> fields = JDBCUtil.getFieldsWithList("nms", "detection_info");
		String[] temp=new String[fields.size()];
		
		temp=fields.toArray(temp);
		List<Map<String,Object>> data = JDBCUtil.getData("nms", "detection_info", 0, 500, temp);
		
		pipe.addAll(data);
		List<Map<String,Object>> popList = pipe.popList(100);
		
		JDBCUtil.insertToPgsql("detection_info", popList, temp);
	}
}
