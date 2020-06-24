package com.chris.web.service;

import com.chris.web.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component

public class HbaseService {
    private HConnection connection;

    public HbaseService() {
        //创建hadoop配置文件对象
        Configuration configuration = new Configuration();

        //指定zk地址
        configuration.set("hbase.zookeeper.quorum", Config.getString("hbase.zookeeper.quorum"));


        try {
            //建立连接   和zk的连接
            connection = HConnectionManager.createConnection(configuration);
            System.out.println("连接建立成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Result get(String tableName, String rowkey) {
        Result result = null;
        try (HTableInterface table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(rowkey.getBytes());
            result = table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public ResultScanner scan(String tableName) {
        ResultScanner scanner = null;
        try (HTableInterface table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();

            scanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return scanner;
    }

    public void put(String tableName, Put put) {

        try (HTableInterface table = connection.getTable(TableName.valueOf(tableName))) {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
