package com.erongda.bigdata.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * 基于Java语言使用Jedis API操作Redis数据库，进行CRUD操作
 */
public class JedisDemo {

    public static void main(String[] args) {

        // 1. 获取JedisPool实例对象
        JedisPool jedisPool = JedisPoolUtil.getJedisPoolInstance() ;

        // 2. 获取Jedis连接对象
        Jedis jedis = jedisPool.getResource() ;

        // 3. 存储各个省份销售订单额
        jedis.hset("orders:total", "bj", "999999");
        jedis.hset("orders:total", "sh", "33443");
        jedis.hset("orders:total", "hz", "123112");
        jedis.hset("orders:total", "sz", "123543");

        // 4. 关闭连接
        JedisPoolUtil.release(jedis);
    }
}
