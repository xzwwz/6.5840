#!/bin/bash

# 测试次数
TEST_COUNT=1500
# 成功计数器
SUCCESS=0
# 失败计数器
FAIL=0

echo "开始执行 $TEST_COUNT 次测试: go test -run 3A"

for ((i=1; i<=$TEST_COUNT; i++))
do
    echo -n "第 $i 次测试... "
    
    # 执行测试并捕获输出
    output=$(go test -run 3A -race 2>&1)
    result=$?
    
    if [ $result -eq 0 ]; then
        echo "通过"
        ((SUCCESS++))
    else
        echo "失败"
        ((FAIL++))
        # 保存失败日志
        echo "$output" > "test_fail_$i.log"
        echo "失败日志已保存到 test_fail_$i.log"
    fi
done

# 输出统计结果
echo ""
echo "测试完成！"
echo "总测试次数: $TEST_COUNT"
echo "通过次数: $SUCCESS"
echo "失败次数: $FAIL"
echo "通过率: $((SUCCESS * 100 / TEST_COUNT))%"