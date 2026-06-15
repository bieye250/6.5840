#!/usr/bin/env bash
# Run the specified `go test` multiple times in parallel.
# Usage: ./run_until_fail.sh [TestPattern] [MaxRuns]
# Default TestPattern: TestUnreliableChurn3C
# Default MaxRuns: 5

TEST_PATTERN=${1:-TestUnreliableChurn3C}
MAX_RUNS=${2:-5}

# 创建临时目录存放每个测试的输出
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

echo "Running $MAX_RUNS instances of 'go test -run $TEST_PATTERN' in parallel..."

# 启动所有后台测试任务
for ((i=1; i<=MAX_RUNS; i++)); do
    logfile="$tmpdir/run_$i.log"
    go test -run "$TEST_PATTERN" > "$logfile" 2>&1 &
done

# 等待所有后台任务结束
wait

pass=0
fail=0

# 检查每个任务的输出结果
for ((i=1; i<=MAX_RUNS; i++)); do
    logfile="$tmpdir/run_$i.log"
    # 取最后两行判断（与原脚本逻辑一致）
    last_two_lines=$(tail -2 "$logfile" 2>/dev/null)

    if echo "$last_two_lines" | grep -q "PASS"; then
        pass=$((pass + 1))
        echo "Run #$i: PASS"
        echo "$last_two_lines"
    elif echo "$last_two_lines" | grep -q "FAIL"; then
        fail=$((fail + 1))
        echo "Run #$i: FAIL"
        echo "$last_two_lines"
        mv "$logfile" error_$i.log
    else
        fail=$((fail + 1))
        echo "Run #$i: UNKNOWN (treated as FAIL)"
        echo "$last_two_lines"
        mv "$logfile" error_$i.log
    fi
done

echo "----------------------------------------"
echo "Results: $pass passed, $fail failed out of $MAX_RUNS runs"

if [ "$fail" -gt 0 ]; then
    echo "Some runs failed."
    exit 1
else
    echo "All runs passed."
    exit 0
fi