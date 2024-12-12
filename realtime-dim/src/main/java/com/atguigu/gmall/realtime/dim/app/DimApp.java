package com.atguigu.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.dim.function.HbaseSinkFunction;
import com.atguigu.gmall.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

public class DimApp  extends BaseApp {
    public static void main(String[] args) throws Exception {

        new DimApp().start(10088,4,"dim_app",Constant.TOPIC_DB);

    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        // TODO: 2024/10/16 对业务流中 数据类型进行转换 JSonStr ->JSONOBJ
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String db = jsonObj.getString("database");
                String type = jsonObj.getString("type");
                String data = jsonObj.getString("data");
                if ("gmall".equals(db)
                        && ("insert").equals(type)
                        || "update".equals(type)
                        || "delete".equals(type)
                        || "bootstrap-insert".equals(type)
                        && data != null
                        && data.length() > 2
                ) {
                    out.collect(jsonObj);
                }

            }
        });

        jsonObjDS.print();
        //使用Flink CDC 读取配置表中的配置信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2024_config", "table_process_dim");
        //读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        // TODO: 2024/10/16 对配置流中的数据类型进行转换 jsonstr -> 实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDs = mysqlStrDS.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String jsonStr) throws Exception {
                //为了处理方便 先将jsonStr转换为jsonobj
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    //对配置表进行了一次删除操作 从before属性中获取删除前的配置信息
                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                } else {
                    //对配置表进行读取 添加 修改操作 从after属性中获取最新的配置信息
                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);

                return tableProcessDim;
            }
        }).setParallelism(1);
        tpDs.print();

        SingleOutputStreamOperator<TableProcessDim> tpDS = tpDs.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            @Override
            public TableProcessDim map(TableProcessDim tp) throws Exception {
                //获取配置表进行的操作
                String op = tp.getOp();
                //获取hbase中维度表的表名
                String sinkTable = tp.getSinkTable();
                //获取在Hbase中的建表的列族
                String[] sinkFamilies = tp.getSinkFamily().split(",");
                if ("d".equals(op)) {
                    //从配置表中删除了一条数据 将hbase中对应的表删除掉
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                } else if ("r".equals(op) || "c".equals(op)) {
                    //配置表中读取了一条数据或者向配置表中添加了一条配置 在hbase中执行建表
                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                } else {
                    //对配置表中的配置信息进行了修改 先从hbase中将对应的表删除掉 在创建新表
                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                }
                return tp;
            }
        }).setParallelism(1);
        tpDS.print();
        // TODO: 2024/10/16 将配置流中的配置信息进行广播 --broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor = new MapStateDescriptor<>("MapStateDescriptor", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        // TODO: 2024/10/16 将主流业务数据和广播流配置信息进行关联 --connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        // TODO: 2024/10/16 处理关联后的数据（判断是否为维度）
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(new TableProcessFunction(mapStateDescriptor));
        // TODO: 2024/10/16 将维度数据同步到hbase表中
        dimDS.print();
        dimDS.addSink(new HbaseSinkFunction());
    }
}
