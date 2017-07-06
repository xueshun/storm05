package xue.redis.storm;

import java.io.Serializable;
import java.util.Map;

import com.esotericsoftware.kryo.util.ObjectMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

public class RedisOperations implements Serializable {

	private static final long serialVersionUID = 1L;
	Jedis jedis = null;
	
	public RedisOperations(String redisIP, int port){
		jedis = new Jedis(redisIP,port);
	}
	
	public void insert(Map<String,Object> record,String id){
		try {
			jedis.set(id, new ObjectMapper().writeValueAsString(record));
		} catch (JsonProcessingException e) {
			System.out.println("Record not persist into ");
			e.printStackTrace();
		}
	}

}
