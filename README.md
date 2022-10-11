# lab1 记录
共享文件系统、利用RPC实现的CS架构分布式word count

基于map reduce实现一个word count,开启一个coordinator和多个worker,worker通过RPC request向coordinator请求任务.coordinator响应请求并且分配任务.

总共有两种任务:MapTask && ReduceTask. 当coordinator接收到任务请求(不带有taskType)时,coordinator根据当前任务完成情况分配mapTask or ReduceTask.

## coodinator 处理逻辑

- 初始化:将待map的文件读入mapTask中

- coordinator根据workerId维护分配的任务记录.

- 当coordinator接收到worker发送的任务请求时,
    - 如果该worker首次发送请求,则coordinator给初次发送请求的worker分配一个workerId,然后任务结束。
    - 如果该worker不是首次发送请求,则coodinator任务该worker已经完成上一次任务.因此需要执行任务完成操作.根据上一次任务的taskType执行逻辑,如果是:
        - mapTask,则将其完成的文件名写入reduceTaskList
        - reduceTask,不执行操作.
        - workerIdAssignmentTask,不执行操作
    - 执行完任务完成的操作后,执行任务分配的操作:
        - 如果mapTask队列和reduceTask队列都已经为空,则返回一个waitTask
        - 如果mapTask队列和reduceTask队列有一个为空,则assign不为空的task
        - 如果mapTask队列和reduceTask队列都不为空,则随机assign一个task.

## worker 处理逻辑

- worker不断地向coordinator请求task,根据coordinator发送的taskType执行相应的动作

- 当worker收到workerIdAssignmentTask时,保存该id,然后结束任务;

- 当worker收到mapTask时,读取文件里的每个词并且置出现次数为1,把这个结果写到一个中间文件`mr-${workerId}-${i}-${n}`, i为0~nReduce中的一个数,n为该worker执行mapTask的次数. 任务完成后任务结束;

- 当worker收到reduceTask时,coordinator将此时ready被reduce的文件(e.g `mr-x-y`传送给worker,worker将`mr-x-y`与目标文件`mr-y`合并,写在`mr-y`中.任务完成后任务结束

- 当worker收到waitTask时,sleep 10s. 任务完成后任务结束

- 任务结束后worker继续请求任务

- 当worker连续收到3个waitTask时,认为任务全部完成,程序退出.
