# 目的
- 是提高代码运行效率，程序可能有很多时间都是空闲的，但是还是希望传入数据到接收到结果的时间差越短越好。
- actor模式的线程和进程。代码来源于《python cookbook》和"pytorch的dataloader"
- 大体思想：
    - 构建进程，进程中不断遍历队列，获取到数据之后就处理，否则继续遍历
    - 队列就是特洛伊木马，一个进入的木马，一个出来的木马
    - 和pytorch的不一样的地方：
        - pytorch的出口处也是queue.get(timeout=n)来实现的，这种对于快速获取值不太友好。
        - 使用了信号量来控制：multiprocessing.Condition() notify_all() & wait()

- 和普通的调用方式的比较
    - Pool的使用
    - concurrent的使用：from concurrent.futures import ProcessPoolExecutor, TimeoutError, ThreadPoolExecutor
    - concurrent需要调用函数是"孤立"的函数，"孤立是待运行的函数不需要与程序中的其他部分共享状态"——effective python
