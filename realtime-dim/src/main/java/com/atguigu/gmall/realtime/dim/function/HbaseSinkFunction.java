package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

public class HbaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

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
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
        JSONObject jsonObj = tup.f0;
        TableProcessDim tableProcessDim = tup.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");
        //获取操作的HBase表的表名
        String sinkTable = tableProcessDim.getSinkTable();
        //获取rowkey
        String rowkey = jsonObj.getString(tableProcessDim.getSinkRowKey());
//                判断对业务数据库味道进行了什么操作
        if("delete".equals(type)){
//                    从业务数据库维度表中做了删除操作 需要将hbase维度表中对应的几率也删除掉
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowkey);
        }else{
//                    如果不是delte可能得类型 有insert update bootstrap-insert 上述操作对应的都是Hbase表中put数据
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowkey,sinkFamily,jsonObj);
        }
    }
}
