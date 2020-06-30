package com.chris.web.controller;

import com.chris.web.common.Config;
import com.chris.web.bean.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class DataController {


    /**
     * 获取性别占比
     */
    @RequestMapping("/getGenderCount")
    public ArrayList<GenderCount> getGenderCount(String id) {
        ArrayList<GenderCount> genderCounts = new ArrayList<>();

        String key = "gender:" + id;

        //查询redis获取性别占比
        Jedis jedis = new Jedis(Config.getString("redis.host"), 6379);
        Map<String, String> map = jedis.hgetAll(key);

        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key1 = entry.getKey();
            long value = Long.parseLong(entry.getValue());

            if ("m".equals(key1)) {
                genderCounts.add(new GenderCount("男", value));
            } else if ("f".equals(key1)) {
                genderCounts.add(new GenderCount("女", value));
            } else {
                genderCounts.add(new GenderCount("其它", value));
            }
        }


        return genderCounts;
    }


    /**
     * 获取词云图
     */
    @RequestMapping("/getWordCloud")
    public ArrayList<Word> getWordCloud(String id) {
        ArrayList<Word> words = new ArrayList<>();

        String key = "word_cloud:" + id;

        //查询redis获取性别占比
        Jedis jedis = new Jedis(Config.getString("redis.host"), 6379);

        Map<String, String> map = jedis.hgetAll(key);

        for (Map.Entry<String, String> entry : map.entrySet()) {
            String word = entry.getKey();
            Integer count = Integer.parseInt(entry.getValue());

            words.add(new Word(word, count));
        }

        return words;
    }

    /**
     * 获取舆情走势
     */
    @RequestMapping("/getRealTimeSentiment")
    public Real getRealTimeSentiment(String id) {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", Config.getString("hbase.zookeeper.quorum"));
        Real real = new Real();
        try {
            //丽娜姐regionserver,  负责表的增删改查
            HConnection connection = HConnectionManager.createConnection(conf);

            HTableInterface table = connection.getTable("comment_sentiment");

            Get get = new Get(id.getBytes());
            get.addFamily("info".getBytes());
            //如果不指定版本号，默认只查询一个版本的数据
            get.setMaxVersions(50);


            ArrayList<String> xlist = new ArrayList<>();
            ArrayList<Integer> y1list = new ArrayList<Integer>();
            ArrayList<Integer> y2list = new ArrayList<Integer>();
            ArrayList<Integer> y3list = new ArrayList<Integer>();

            real.setX(xlist);
            real.setY1(y1list);
            real.setY2(y2list);
            real.setY3(y3list);

            Result result = table.get(get);
            List<Cell> columnCells = result.getColumnCells("info".getBytes(), "real".getBytes());

            for (Cell columnCell : columnCells) {
                long timestamp = columnCell.getTimestamp();
                Date date = new Date(timestamp);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh");
                String x = format.format(date);
                xlist.add(x);

                String value = Bytes.toString(CellUtil.cloneValue(columnCell));

                HashMap<String, Integer> map = new HashMap<>();
                map.put("0.0", 0);
                map.put("1.0", 0);
                map.put("2.0", 0);

                for (String kv : value.split("\\|")) {
                    String k = kv.split(":")[0];
                    Integer v = Integer.parseInt(kv.split(":")[1]);
                    map.put(k, v);
                }
                y1list.add(map.get("0.0"));
                y2list.add(map.get("1.0"));
                y3list.add(map.get("2.0"));

            }

            Collections.reverse(real.getX());
            Collections.reverse(real.getY1());
            Collections.reverse(real.getY2());
            Collections.reverse(real.getY3());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return real;

    }


}
