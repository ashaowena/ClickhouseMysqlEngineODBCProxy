# 原因分析
通过ODBC协议访问Clickhouse时，一些服务端可能会因为数据长度问题报错，无法拿到数据集，如下图：![在这里插入图片描述](https://img-blog.csdnimg.cn/778edc0f2f644f4ca961ff48578da80e.png#pic_center)
这句报错的意思是，在获取数据集的时候，允许列a的最大数值是4，但实际长度是12。

为了方便探究原因，这里使用了Clikhouse的Mysql引擎（9004端口），并把同样的一句SQL分别发送给Mysql和Clickhouse，然后对比各自返回的报文，看看有什么不同![在这里插入图片描述](https://img-blog.csdnimg.cn/b58bd8f62beb42abbc8809954975ffbc.png#pic_caenter)
![在这里插入图片描述](https://img-blog.csdnimg.cn/4dca6c3cc2274b9e9386a9c56a2587b4.png#pic_center)

下一步，对比Clickhouse的MysqlEngine和Mysql5.7返回的结果集有什么差异，如下图：
![在这里插入图片描述](https://img-blog.csdnimg.cn/28aba51b44f7497390be7c5e5558c062.png#pic_center)
可以看出，除去clickhouse后面的附加信息外，其余报文大都是相同的，第一个不同的地方是字段类型段，mysql对数据a的描述为-3(utf8)，clickhouse对数据a的描述为-2（utf8mb4）。
第二个不同的地方则是用4个字节表示的字段最大长度，mysql算出数据a的长度为4，又因为只有这一行数据，所以最大数据也是4，clickhouse因为俄罗斯人考虑到效率问题就直接省略成0了，这就是造成问题的原因。
还有第三个不同的地方就是mysql认为字段a的类型为固定长度的varchar型数组，clickhouse认为字段a的类型为char类型，这点其实也很重要，一些客户端会根据类型校对长度，可能是sqlserver较为严格的原因，字段最大长度即便改成比真实长度大也会报错

# 解决办法
通过代理中间件，把报文中的字段信息修正成实际的最大信息，编码统一设置为utf8，格式统一设成varchar，即可。
这里使用了Vertx作为中间件，拦截clickhouse端返回的结果集，并在接收完整后，挑选出每个字段中最大数据的数据长度，并修改报文，重新发送给客户端。

这个问题不管使用mysql协议还是clickhouse原生的http协议下都会存在，为了方便开发和对比，这里采用了clickhouse mysql engine的mysql协议，也就是说用mysql odbc连接这个代理中间件即可：

![在这里插入图片描述](https://img-blog.csdnimg.cn/24d2b68071e648028e6865f2bd6634f3.png#pic_center)
这里要和9004区别开来，19004为VertX代理中间件的端口，客户端（sqlserver链接服务器等）通过这个ODBC拿数即可
# 最后小结
这个代理中间件在查询结果集很大的时候毫无疑问有oom的风险，这里也能看出俄罗斯人为何会省略最大长度等元信息，毕竟在大数据的背景下不可能拿着整个结果集一点一点的比较出到底哪行数据长度最大，最大长度是多少，旦作为一些客户端而言，没有最大长度可能意味着报文信息有误，不符合协议规范，不方便管理内存等问题。

# 原文博客
## https://blog.csdn.net/cxlswh/article/details/128019224