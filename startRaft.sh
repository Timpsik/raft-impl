#!/bin/bash

## Check if 3 arguments are given
if [ "$#" -ne 3 ]; then
	echo "Illegal number of parameters, 3 expected. Number of servers, time to reserve nodes and username"
	exit 2
fi

## Number of Zookeeper servers
numberOfServers=$1;
if [ $numberOfServers -lt 1 ]; then
	echo "Atleast one node should be requested for Zookeeper cluster. Currently, you asked the cluster size to be $numberOfServers."
	exit 2
fi
echo "Number of nodes requested: $1";

## Time to reserve the nodes, hh:mm:ss
timeToReserve=$2;

## Username of the caller
username=$3;


module load prun

## Reserve the nodes
preserve -# $numberOfServers -t $timeToReserve

## Wait for reservation to finalise
sleep 5

reservationState=$(preserve -llist | grep $username | awk '{print $7}')

echo "Reservation state: $reservationState"

if [ "$reservationState" = "PD" ]; then
	echo " There aren't $numberOfServers nodes available to be reserved, try again later"
	scancel -u $username
	exit 2
fi

## Get the reserved nodes, expect that there aren't any other reservations for the user and nodes are available
nodes=$(preserve -llist | grep $username | awk '{ for (i=9; i<=NF; i++) print $i }')

## Create array of the nodes
nodeNumber=0;
for node in ${nodes}
do
	nodeArray[$nodeNumber]=$node;
	nodeNumber=$nodeNumber+1;
done


## Create Zookeeper configurations
for (( i=1; i <= $numberOfServers; i++))
do
	echo "Establishing connection to ${nodeArray[$i-1]}"
ssh ${nodeArray[$i-1]} <<-EOF
	if [ ! -d "/local/$username" ]; then
		mkdir "/local/$username"
	else
		rm -r "/local/$username"
		mkdir "/local/$username"
	fi
	mkdir "/local/$username/raft"
	cp -r "/var/scratch/$username/raft-impl/raft_deploy" "/local/$username"
	mkdir "/local/$username/raft/snapshots"
EOF
done
## From https://stackoverflow.com/a/17841619
function join_by { local d=$1; shift; local f=$1; shift; printf %s "$f" "${@/#/$d}"; }

## Get the Zookeeper cluster address
clusterAddress=$(join_by ":2181," $nodes);
clusterAddress+=":2181";

echo $clusterAddress

## Start the Raft servers
for (( i=1; i <= $numberOfServers; i++))
do
	echo "Starting server in node ${nodeArray[$i-1]}"
ssh ${nodeArray[$i-1]} <<-EOF
	cd /local/$username
	nohup java -jar /local/$username/raft_deploy/raft-0.0.jar   $clusterAddress $(($i-1)) &> /local/$username/output.log &
EOF

done

echo "Zookeeper cluster should be started"
