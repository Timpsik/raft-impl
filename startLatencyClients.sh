#!/bin/bash

## Check if 3 arguments are given
if [ "$#" -ne 1 ]; then
	echo "Illegal number of parameters, 3 expected. Number of clients on node, username and client write rate."
	exit 2
fi

## Username of the caller
username=$1;

module load prun

## Get Zookeeper nodes, they are since 9th column, expect them to be the first reservation of the user.
zookeeperNodes=$(preserve -llist | grep $username | sed -n '1p'| awk '{ for (i=9; i<=NF; i++) print $i }')

## Get the current time since Epoch in milliseconds
currentTime=$(date +%s%3N);

## Start the benchmark in 1 minute from now
let startTime=$currentTime+1000*60;


echo "Benchmark Start time: $(date -d @"$((startTime/1000))")";
echo $currentTime

## From https://stackoverflow.com/a/17841619
function join_by { local d=$1; shift; local f=$1; shift; printf %s "$f" "${@/#/$d}"; }

## Get the Zookeeper cluster address
clusterAddress=$(join_by ":2181," $zookeeperNodes);
clusterAddress+=":2181";

## Reserve nodes for clients
preserve -# 1 -t 00:15:00

## Wait 5 seconds for reservation to show up
sleep 5

## Check if nodes are available to use, if it isn't cancel the reservation.
reservationState=$(preserve -llist | grep $username | sed -n '2p' | awk '{print $7}')

if [ "$reservationState" = "PD" ]; then
	echo " There aren't 1 nodes available to be reserved for clients, try again later."
	scancel -u $username -t "PD"
	exit 2
fi

## Get the client nodes, expect it to be the second reservation for the user
nodes=$(preserve -llist | grep $username | sed -n '2p' | awk '{ for (i=9; i<=NF; i++) print $i }')
clients=10

echo "Cluster address: $clusterAddress";
echo "Number of total clients: $clients"

## Start the clients in each node
for node in ${nodes}
do
echo "Connecting to node $node"
## Copy the Zookeeper client
## Delete the data from previous run
## Create the directory again
## Start the clients
ssh $node<<-EOF
	if [ ! -d "/local/$username" ]; then
		mkdir "/local/$username"
	else
		rm -r "/local/$username"
		mkdir "/local/$username"
	fi
	mkdir "/local/$username/raft"
	cp -r "/var/scratch/$username/raft-impl/client_deploy" "/local/$username"
	for (( i=0; i < 10; i++))
	do
		echo "starting client \${i}"
		nohup java -jar /local/$username/client_deploy/client-0.0.jar   $clusterAddress \$i $startTime &> "/local/${username}/output_\${i}.log" &
	done
EOF
done

