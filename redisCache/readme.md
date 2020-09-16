# 说明
- 本实验模拟的是对于任一一个字符串（自身有id和hash值），都会找分配一个汉明距离最近且小于10的字符串的fp作为自己的fp，否则就是自己

- 实现1：worker2
    - 插入使用redis.set
    - 取出使用redis.scan
    - 结果：对于测试数据，插入到取出sleep 15ms勉强不会有漏网之鱼

- 实现2：worker
    - 取出使用了scan和mget
    - 据组内的大神讲scan(count=1000)也不会有刚插入不能被检索到的这种情况出现，但是测试好像还是有明显时间差。

- 实现3：worker3
    - 插入取出使用hset和hget，是分桶的插入。
    - 这个仍有时间差，不过很短2ms，记不太清了？中间稍微处理一下基本上就没啥问题了。而且还不会占用整个DB
    - 最后也是采用的这种方式

- check是结果分析
    - 一是看有多少是漏网之鱼；
    - 另一方面看他分配到的计算结果。
- test1、test2、test3
    - 顺序和多线程调用
    - 简单的调用，test2不太好用，但是没管它。1和3足够了。后面还有机会接触到系统一点的并发的写法。

- gen**
    - 生成hash值，可以控制生成的hash值的汉明距离

- main**
    - 测试函数。