实现了 MapReduce 

主要包含三个部分：master,  worker, 用户的 map函数 和 reduce函数（plugin）

主要流程:

1. 启动 RPC 服务器，接受 woker 传来的`请求任务`调用，master给worker分配map任务。
2. 等待 所有map任务都完成，master整合中间文件，再继续接受 woker 传来的`请求任务`调用，给worker分配reduce任务。
3. 直到所有reduce任务都完成，master通知worker退出。

容错：

- 对10秒内没有完成的任务重置，重新派发给另一个worker。
- master 维护的数据结构要加锁，因为 golang 的 RPC 是并发的。
- worker通知master完成任务之前，要保证文件已经准备好。
- master在map任务都执行完成后，检查下文件才进入下一阶段，reduce任务都执行后也检查下文件，才算结束。

