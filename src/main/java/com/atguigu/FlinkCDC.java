package com.atguigu;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        // 1.获取Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1开启checkpoint
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://dizhi...."));

        // 2.通过FlinkCDC构建SoureceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("cdc_test")// 可同时监控多个数据库,不写表名就是监控库中所有的表
//                .tableList("cdc_test.user_info")// 可监控多个表,写的时候一定要库名.表名
                .deserializer(new StringDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        // 3.数据打印
        dataStreamSource.print();

        // 4.启动任务
        env.execute("FlinkCDC");

    }
}
