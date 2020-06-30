# 循环去redis中获取需要爬取的url

import redis

import requests
import json
from kafka import KafkaProducer
import re
import time
import config

# 创建kafka连接
produce = KafkaProducer(bootstrap_servers=config.kafka_bootstrap_servers)

# 创建连接
client = redis.Redis(host=config.redis_host, port=config.redis_port)

print("=========爬虫已启动===========")

# 循环爬取数据
while True:

    time.sleep(3)
    # timeout= 0永不超时
    tuple = client.brpop("search_urls", timeout=0)

    # 解码
    url = tuple[1].decode("utf-8")

    # 获取舆情编号
    sent_id = client.get("flag_" + url).decode("utf-8")

    print("正在爬取：" + url)

    # 就获取数据
    result = requests.get(url)

    # 转换成json对象
    js = json.loads(result.text)

    for card in js["data"]["cards"]:

        re_h = re.compile('</?\w+[^>]*>')  # 去掉HTML标签

        mblog = card["mblog"]

        # 微博文章数据
        create_date = mblog["created_at"]

        ###########################微博文章数据#####################
        id = mblog["id"]
        mid = mblog["mid"]
        text = mblog["text"]

        text = re_h.sub("", text)

        source = mblog["source"]

        reposts_count = mblog["reposts_count"]  # 转发数
        comments_count = mblog["comments_count"]  # 评价数
        attitudes_count = mblog["attitudes_count"]  # 点赞数

        ###########################微博文章数据#####################

        # 微博用户数据1
        user = mblog["user"]

        ###########################微博用户数据1#####################
        user_id = user["id"]
        user_name = user["screen_name"]
        gender = user["gender"]
        description = user["description"]
        follow_count = user["follow_count"]  # 关注数
        followers_count = user["followers_count"]  # 粉丝数
        ###########################微博用户数据1#####################
        # 将数据写入到kafka,  每一类数据一个topic

        line = "%s|%s|%s|%s|%s|%s|%s|%s" % (
            id, sent_id, user_id, source, reposts_count, comments_count, attitudes_count, text)

        # 微博文章数据写入到kafka
        produce.send(topic="wz", value=line.encode("utf-8"))
        produce.flush()

        # 微博用户数据保存到kafka
        user_str = "%s|%s|%s|%s|%s|%s" % (user_id, user_name, gender, description, follow_count, followers_count)

        produce.send(topic="user", value=user_str.encode("utf-8"))
        produce.flush()
        # 爬取微博评价

        comment_url = "https://m.weibo.cn/comments/hotflow?id=$1&mid=$2&max_id_type=0"

        # 爬取评价数据
        comment_result = requests.get(comment_url.replace("$1", id).replace("$2", mid))
        print(comment_result)
        comment_json = json.loads(comment_result.text)
        print(comment_json)

        # 判断是否有数据
        if "data" in comment_json:
            if "data" in comment_json["data"]:

                for comment in comment_json["data"]["data"]:
                    ###########################微博微博评价数据#####################
                    created_at = comment["created_at"]
                    comment_id = comment["id"]
                    comment_text = comment["text"]

                    comment_text = re_h.sub("", comment_text)

                    like_count = comment["like_count"]  # 点赞数
                    ###########################微博微博评价数据#####################

                    user1 = comment["user"]

                    ###########################微博用户数据2#####################
                    user_id1 = user1["id"]
                    user_name1 = user1["screen_name"]
                    gender1 = user1["gender"]
                    description1 = user1["description"]
                    follow_count1 = user1["follow_count"]  # 关注数
                    followers_count1 = user1["followers_count"]  # 粉丝数
                    ###########################微博用户数据3#####################

                    # 将数据写入到kafka,  每一类数据一个topic

                    # 微博用户数据保存到kafka
                    user_str1 = "%s|%s|%s|%s|%s|%s" % (
                        user_id1, user_name1, gender1, description1, follow_count1, followers_count1)

                    produce.send(topic="user", value=user_str1.encode("utf-8"))
                    produce.flush()
                    # 微博评价数据保存到kafka
                    comment_str = "%s|%s|%s|%s|%s|%s" % (
                        comment_id, sent_id, created_at, like_count, user_id1, comment_text)

                    produce.send(topic="com", value=comment_str.encode("utf-8"))
                    produce.flush()
