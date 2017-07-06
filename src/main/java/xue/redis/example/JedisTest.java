package xue.redis.example;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import redis.clients.jedis.Jedis;

public class JedisTest {
	
	@Test
	public void testJedis(){
		//创建和redis的连接
		Jedis jedis = new Jedis("192.168.1.191",6379);
		
		//存入
		jedis.set("key2", "2");
		
		//取出
		System.out.println(jedis.get("key2"));
		
		List<String> list = jedis.mget("name","age");
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.println(string);
		}
		jedis.close();
	}
}
