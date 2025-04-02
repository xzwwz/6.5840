# lab1

- 将 input file 分成 M 个task文件
- task_request 请求任务
- process_task 处理任务
    - map task:
        - 获取task文件
        - todo：执行mapf 


- filesystem
    - path: "./files"
        - taskpath: path+"/task"  taskfile: taskpath+"/task-id.txt"
        - mapoutputpath: path+"/map" mapoutputfile: mapoutputpath+"/map-out-uid-r.txt
        - reducepath: path+"/reduce" reducefile: reducepath+"/mr-out-r

# lab2

## kv
- server
- client

## lock
- 使用kv服务器实现外部锁
- get put 原子操作实现锁的获取和释放
- 加入退避策略，减小竞争强度
- 优化退避策略，加入随机抖动，减少重试同步问题
- Aquire失败后，若lock已被占用，直接等待最大退避时间，减少重试次数
- 加上计时机制，防止客户端奔溃导致锁无法释放
