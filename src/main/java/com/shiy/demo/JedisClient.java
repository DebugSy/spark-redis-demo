package com.shiy.demo;

import org.apache.parquet.Strings;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;

/**
 * Created by DebugSy on 2019/3/20.
 */
public class JedisClient {

  public long deleteRedis() {
    String url = "192.168.1.83:6379";
    String table = "redis_student_cache";
    if (!Strings.isNullOrEmpty(url)) {
      try {
        String[] hostAndPort = url.split(":");
        String host = hostAndPort[0];
        int port = Integer.valueOf(hostAndPort[1]);
        Jedis jedis = new Jedis(host, port);
        String keyPattern = table.concat(":*");
        Set<String> keys = jedis.keys(keyPattern);
        for (String key : keys) {
          System.out.println(key);
        }
        keys.add(String.format("_spark:%s:schema", table));
        Long rsCode = jedis.del(keys.toArray(new String[keys.size()]));
        return rsCode;
      } catch (Exception e){
        throw new RuntimeException(e);
      }
    }
    return -1;
  }

  public static void main(String[] args) {
    JedisClient jedisClient = new JedisClient();
    long rsCode = jedisClient.deleteRedis();
    System.out.println(rsCode);
  }

}
