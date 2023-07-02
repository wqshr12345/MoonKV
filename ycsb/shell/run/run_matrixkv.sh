#!/bin/bash
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")
max_retries=2
#note: next line should use MatrixKV's link library path, xxx is an example
export LD_LIBRARY_PATH=/xxx
for workload in ${workloads[@]};do
    #重试计数器初始为0
    retry_count=0
    # 添加前缀和后缀到workload_param及log
    workload_param="workloads/matrixkv/${workload}"
    log="result/matrixkv/${workload}/output.txt"
    while true; do
        cmd="shell/exec/ycsb_matrixkv -load -run -db rocksdb -P ${workload_param} -P rocksdb/matrixkv.properties -s >${log}"
        echo "Running command: $cmd"
        eval $cmd 
        # 如果命令成功退出循环
        if [ $? -eq 0 ]; then
            break
        # 命令失败，更新重试计数器
        else
            retry_count=$((retry_count+1))
            # 如果重试次数超过了最大次数，放弃重试
            if [ $retry_count -ge $max_retries ]; then
                echo "Command failed after $retry_count retries, giving up"
                break
            else
                # 继续重试
                echo "Command failed, retrying (attempt $retry_count/$max_retries)"
            fi
        fi
    done
done