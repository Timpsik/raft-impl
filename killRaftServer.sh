# Kill raft server running on the node

raftServerPID=$(ps -aux | grep raft | head -n 1 | awk '{print $2}')
kill $raftServerPID
echo $(($(date +%s%N)/1000000))