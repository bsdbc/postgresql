package com.wwr.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
	//服务器IP地址
	private static String ADDR="192.168.11.67";
	
	//端口
	private static int PORT=6379;
	
	//连接实例的最大连接数
	private static int MAX_ACTIVE=1024;
	
	//控制一个pool最多有多少个状态为idle（空闲的）的jedis实例，默认值是8
	private static int MAX_IDLE=200;
    
	//等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException
    private static int MAX_WAIT = 10000;
    
    //连接超时的时间　　
    private static int TIMEOUT = 10000;

	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
    private static boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;
    
    private static final String AUTH="123";
    
    static{
    	try {
			JedisPoolConfig config=new JedisPoolConfig();
			config.setMaxIdle(MAX_IDLE);
			config.setMaxWaitMillis(MAX_WAIT);
			config.setMaxTotal(MAX_ACTIVE);
			config.setTestOnBorrow(TEST_ON_BORROW);
			jedisPool=new JedisPool(config, ADDR, PORT, TIMEOUT,AUTH);
		} catch (Exception e) {
			// TODO: handle exception
		}
    }
    
    public synchronized static Jedis getJedis(){
    	try {
			if(jedisPool!=null){
				Jedis jedis = jedisPool.getResource();
				return jedis;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return null;
    }
    
    public static void close(Jedis jedis){
    	try {
    		if(jedis!=null){
    			jedisPool.returnResource(jedis);
    		}
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    }
    
}
