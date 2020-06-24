package com.chris.test;

import redis.clients.jedis.Jedis;

public class TestRedis {
    public static void main(String[] args) {

        Jedis jedis = new Jedis("master");
        jedis.auth("123456");

        String count = jedis.hget("word_cloud:3", "北京加油");

        System.out.println(count);


    }
}
