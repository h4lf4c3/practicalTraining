# 调度程序，循环构建url,将url保存到redis中

import redis
from urllib import parse
import time
import config

client = redis.Redis(host=config.redis_host, port=config.redis_port,password=config.redis_pw)

import MySQLdb

# 去mysql中获取舆情列表，动态构建url

bash_url = "https://m.weibo.cn/api/container/getIndex?" \
           "containerid=100103type%3D60%26q%3D$1%26t%3D0&page_type=searchall&page=$2"

print("调度模块已启动")
while True:
    # 创建连接
    con = MySQLdb.connect(host=config.mysql_host, user=config.mysql_user, passwd=config.mysql_password,
                          db=config.mysql_db, charset='utf8')
    # 获取操作游标
    cursor = con.cursor()
    # 获取所有舆情信息
    cursor.execute("select * from tb_sentiment")

    # 取出sql查询到的数据
    rows = cursor.fetchall()

    # 遍历所哟舆情
    for row in rows:
        sent_id = row[0]
        words = row[2]
        words = words.split(",")

        # 循环爬取多个关键字
        for word in words:
            # 对中文进行编码
            wd = parse.quote(word)

            # 替换关键字
            word_url = bash_url.replace("$1", wd)

            for page in range(10):
                url = word_url.replace("$2", str(page))
                print("正在调度：" + url)
                # 将url保存到redis中
                client.lpush("search_urls", url)

                # 保存爬取数据的url和舆情编号的关系
                client.set("flag_" + url, sent_id)

    # 10秒调度一次
    time.sleep(10)
