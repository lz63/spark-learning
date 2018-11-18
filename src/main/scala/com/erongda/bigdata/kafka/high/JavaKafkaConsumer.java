package com.erongda.bigdata.kafka.high;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 使用高级别的Kafka Consumer API 读取Topic中数据
 */
public class JavaKafkaConsumer {

    /**
     * Kafka 的数据消费者对象
     */
    private ConsumerConnector consumerConnector = null ;

    /**
     * 线程池
     */
    private ExecutorService executorPool = null ;


    public JavaKafkaConsumer(String groupId, String zkUrl){
        // 根据传入的参数，构建一个连接Kafka的消费者对象
        this.consumerConnector = this.createConsumerConnector(groupId, zkUrl);
        // 初始化线程池
        this.executorPool = Executors.newCachedThreadPool() ;
    }

    /**
     * 消费Kafka中数据
     * @param topicCountMap
     */
    public void consumerKafkaMessages(Map<String, Integer> topicCountMap){

        // 指定数据的解码器
        Decoder<String> keyDecoder = new StringDecoder(new VerifiableProperties()) ;
        Decoder<String> valueDecoder = keyDecoder ;

        // 获取给定Topic的流式处理的迭集合
        Map<String, List<KafkaStream<String, String>>> consumerTopicStreams =
                this.consumerConnector.createMessageStreams(topicCountMap,
                        keyDecoder, valueDecoder) ;
        /**
         * 针对每个流式数据，应该启动一个线程来实时处理结果流式数据，再次需要线程池
         */
        for(Map.Entry<String, List<KafkaStream<String, String>>>  entry :consumerTopicStreams.entrySet()){

            // 获取Topic名称
            String topicName = entry.getKey() ;

            // 处理该Topic的数据流对象
            for(KafkaStream<String, String> stream: entry.getValue()){
                this.executorPool.submit(
                        new Runnable() {
                            @Override
                            public void run() {

                            }
                        }
                ) ;

            }
        }
    }


    /**
     * 通过传递参数，获取一个Kafka的ConsumerConnector连接器
     * @param groupId
     * @param zkUrl
     * @return
     */
    public ConsumerConnector createConsumerConnector(String groupId, String zkUrl){
        // a. 构建参数
        Properties props = new Properties() ;
        // 指定消费组的ID
        props.put("group.id", groupId) ;
        // 指定ZK地址
        props.put("zookeeper.connect", zkUrl) ;
        // 设置自动更新OFFSET时间间隔
        props.put("auto.commit.interval.ms", "1000") ;

        // b. 创建消费者配置类实例对象
        ConsumerConfig consumerConfig = new ConsumerConfig(props) ;

        // c. 根据参数构建连接对象
        return Consumer.createJavaConsumerConnector(consumerConfig) ;
    }

}
