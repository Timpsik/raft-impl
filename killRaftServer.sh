raftServerPID=$(ps -aux | grep raft | head -n 1 | awk '{print $2}')
kill $raftServerPID
echo $(date +%s%N)