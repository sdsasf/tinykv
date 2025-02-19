#!/bin/bash
export LOG_LEVEL=error
for ((i=1;i<=200;i++));
do
    echo "ROUND $i";
    (GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run ^TestConfChangeUnreliableRecover3B|| true) > ./test_result/round-confChangeUnreliable-$i.txt;
    # 检查文件中是否包含 FAIL
      if tail ./test_result/round-confChangeUnreliable-$i.txt | grep -q "FAIL"; then
          echo "FAIL found in ./test_result/round-confChangeUnreliable-$i.txt, keeping the file."
      else
          echo "No FAIL found in ./test_result/round-confChangeUnreliable-$i.txt, deleting the file."
          rm ./test_result/round-confChangeUnreliable-$i.txt
      fi
      rm -rf /tmp/*test-raftstore*
done
