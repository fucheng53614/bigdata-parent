# BigData-spark
<p>数据从用户操作视频软件的任何动作产生，kafka的topic区分了不同的动作。
这是一个从kafka到SparkStreaming实时消费通处理，中间处理通过TaskChain轮训处理多个Task，用户只需要实现Task接口处理数据累加到数据库即可。offset也保存在数据库中，方便异常回滚。后续将补充离线的计算操作。  
</p>

<p>如果你有什么好的建议可以与我联系或者补充代码。</p>
<p>QQ：200953614  </p>
<p>email: fucheng53614@qq.com</p>  

### 需要的组件
```
apache-phoenix-4.15.0-HBase-1.3-bin.tar.gz
spark-2.3.4-bin-hadoop2.7.tgz
hbase-1.3.5-bin.tar.gz
zookeeper-3.4.14.tar.gz
hadoop-2.7.7.tar.gz
jdk-8u221-linux-x64.rpm
kafka_2.11-2.1.1.tgz
```


##### 搭建phoenix
```
cp $PHOENIX_HOME/phoenix-4.15.0-HBase-1.3-server.jar $HBASE_HOME/lib
$HBASE_HOME/bin/stop-hbase.sh
$HBASE_HOME/bin/start-hbase.sh

# client
<dependency>
    <groupId>org.apache.phoenix</groupId>
    <artifactId>phoenix-server-client</artifactId>
    <version>4.15.0-HBase-1.3</version>
    <scope>system</scope>
    <systemPath>/home/hadoop/phoenix/phoenix-4.15.0-HBase-1.3-client.jar</systemPath>
</dependency>
```

