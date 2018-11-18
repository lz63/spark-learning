前期准备：
=========================================================================
    a. 启动Kafka Topic
         bin/kafka-server-start.sh -daemon config/server9092.properties
         bin/kafka-server-start.sh -daemon config/server9093.properties
         bin/kafka-server-start.sh -daemon config/server9094.properties
    b. 启动Redis
        bin/redis-server redis.conf
    c. 创建adTopic
        bin/kafka-topics.sh --create --zookeeper bigdata-training01.erongda.com:2181/kafka --replication-factor 2 --partitions 3 --topic adTopic

代码 ->  AdClickRealTimeStateSparkV1
=========================================================================
    -a. 创建上下文
        SparkContext(调度每批次RDD执行)、StreamingContext(读取流式实时数据)
    -b. Kafka集成形成DStream
        采用Direct方式从Kafka Topic中读取数据
    -c. 数据格式转换
        将从Kafka读取的<文本数据>转换封装到<AdClickRecord>中
    -d. 黑名单的更新操作
        黑名单存储在Redis中，在某个时间范围内某个用户点击所有广告的次数 大于 100
        过滤<白名单>数据，白名单数据模拟产生数据（未从Redis中读取）
    -e. 过滤黑名单用户点击数据
        Reduce过滤，RDD的leftOuterJoin操作
        1. 将数据转换为Key/Value类型RDD，方便按照Key进行数据Join
        2. 调用leftOuterJoin/RightOuterJoin
        3. 调用filter过滤数据
        4. map数据转换
    -f. 实时累加统计各个广告点击流量
        (date, 省份, 城市， 广告), 点击量
        使用updateStateByKey API实现，考虑性能问题（计数器Counter）
        将数据输出到关系型数据库MySQL表中 -> 插入 Or 更新 方式

代码 ->  AdClickRealTimeStateSparkV2
=========================================================================
    针对 AdClickRealTimeStateSparkV1进行代码优化，如下优化：
    -a. 状态统计<维度信息>封装
        Case Class StateDimension
            date, province, city, adId
    -b. 在Redis中创建白名单数据库并添加白名单，类型为set
    -c. 更新黑名单操作
        从Redis中读取白名单数据，使用jedis.smembers()方法读取
        将白名单保存到List集合中通过rdd.sparkContext.broadcast(whiteListUsers)广播，减少资源消耗


代码 ->  AdClickRealTimeStateSparkV3
=========================================================================
    -a. 将广告实时累计Top5保存到MySQL中
        1. 直接插入，主键为日期、省份、广告、次数，数据会多
        2. 插入或更新，以日期、省份、广告为主键、统计为values，数据也会多，因为后一批次的TOP5可能有新的，会插入表中
        3. 先删除表中数据再插入，会出现昨天的数据被删除
        4: 通过对批时间过滤进行删除（只删除上个批次的数据，当时间批次为1s时可以，超过1s时凌晨会多些数据，凌晨的时候昨天和今天的数据都会出现）

代码 ->  AdClickRealTimeStateSparkV4
=========================================================================
    -a. 优化：添加一个字段：序号（用于表示当前广告在此省份的降序），主键设置为编号、日期、省份，插入或更新
        减少删除批次步骤，在凌晨时不会出现数据多的情况
    -b. 实时统计最近10分钟的某广告点击流量
        adDStream.transform()对rdd操作
        rdd.mapPartitions()将数据转换为广告Id和1的二元组，preservesPartitioning表示保持原有的分区数，默认为false，避免分区数改变产生shuffle
        窗口聚合统计：
        reduceByKeyAndWindow(reduceFunc: (V, V) => V, invReduceFunc: (V, V) => V, windowDuration: Duration, slideDuration: Duration)
        reduceFunc:定义聚合方式
        invReduceFunc:定义反转方式，用老的窗口+新的rdd-老的rdd，得到新的窗口
        windowDuration:窗口时间
        slideDuration:滑动时间
    -c. 将窗口统计结果保存到MySQL数据库中
        (批次开始时间, 批次结束时间, 批次处理时间, 广告id, 点击量)
        直接插入的方式


    SimpleDateFormat线程不安全



