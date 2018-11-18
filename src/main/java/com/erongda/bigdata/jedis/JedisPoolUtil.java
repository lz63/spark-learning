package com.erongda.bigdata.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.InputStream;
import java.util.Properties;

/**
 * 编写JedisPool工具类，以便后续方便使用
 */
public class JedisPoolUtil {
	
	// 0: 创建一个JedisPool连接池   volatile
	// 被volatile 修饰的变量，不会被本地线程缓存，对该变量的读写都是直接操作共享内存的
	/**
	 * Java关键字transient和volatile小结
	 * 		-a. 综述：
	 * 			transient和volatile两个关键字一个用于对象序列化，一个用于线程同步
	 * 		-b. transient
	 * 			transient是类型修饰符，只能用来修饰字段。在对象序列化的过程中，标记为transient的变量不会被序列化。
	 * 			把一个对象的表示转化为字节流的过程称为串行化（也称为序列化，serialization）;
	 * 			从字节流中把对象重建出来称为反串行化（也称为为反序列化，deserialization）。
	 * 			transient 为不应被串行化的数据提供了一个语言级的标记数据方法。
	 * 		-c. volatile -------   在两个或者更多的线程访问的成员变量上使用volatile
	 * 			volatile 也是变量修饰符，只能用来修饰变量。
	 * 			volatile修饰的成员变量在每次被线程访问时，都强迫从共享内存中重读该成员变量的值。
	 * 			而且，当成员变量发生变 化时，强迫线程将变化值回写到共享内存。
	 * 			这样在任何时刻，两个不同的线程总是看到某个成员变量的同一个值。
	 */
	private static volatile JedisPool jedisPool = null ;
	
	// 构造方法私有化
	private JedisPoolUtil(){}
	
	/**
	 * 使用静态代码块进行加载配置文件信息
	 */
	static{
		// 加载配置文件
		InputStream inStream = JedisPoolUtil.class.getClassLoader() //
				.getResourceAsStream("redis.properties") ;
		// 创建Properties实例，读取配置信息
		Properties props = new Properties();
		try{
			props.load(inStream);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		// 创建池子的配置对象，将配置信息放到对象中
		GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
		// 设置 最大连接数
		poolConfig.setMaxTotal(Integer.valueOf(props.getProperty("redis.maxTotal")));
		// 设置最大的空闲数
		poolConfig.setMaxIdle(Integer.valueOf(props.getProperty("redis.maxIdle")));
		// 设置最小的空闲数
		poolConfig.setMinIdle(Integer.valueOf(props.getProperty("redis.minIdle")));
		
		// 创建一个JedisPool连接池
		jedisPool = new JedisPool( //
			poolConfig, //
			props.getProperty("redis.url"), //
			Integer.valueOf(props.getProperty("redis.port")) //
		);
	}
	
	/**
	 * 返回JedisPool连接池对象实例
	 * @return
	 */
	public static JedisPool getJedisPoolInstance(){
		return jedisPool ;
	}
	
	/**
	 * 释放Jedis连接对象到连接池中
	 * @param jedis
	 */
	public static void release(Jedis jedis){
		// 将Jedis连接对象放回到池中
		jedis.close();
	}

}
