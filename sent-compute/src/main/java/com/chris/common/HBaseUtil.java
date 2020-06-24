package com.chris.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;

public class HBaseUtil {

    private static HConnection connection;

    static {
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

    public static HConnection getConnection() {
        return connection;
    }
}
