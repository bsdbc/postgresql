package com.wwr.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Pipe<T> {
	private  BlockingQueue<T> pipe=new LinkedBlockingDeque<T>();
	
	public void push(T t) throws InterruptedException{
		pipe.put(t);
	}
	
	public void addAll(Collection<? extends T> list){
		pipe.addAll(list);
	}
	public T pop(){
		return pipe.poll();
	}
	
	public List<T> popList(int size){
		List<T> list = new ArrayList<T>();
	 	pipe.drainTo(list,size);
	 	return list;
	}
	
	public synchronized int getSize(){
		return pipe.size();
	}
}
