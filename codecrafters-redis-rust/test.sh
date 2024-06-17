#!/bin/bash

set -x

cargo run -- --port 6481 --replicaof "localhost 6379" &
pid1=$!

cargo run -- --port 6482 --replicaof "localhost 6379" &
pid2=$!

cargo run -- --port 6483 --replicaof "localhost 6379" &
pid3=$!

cargo run -- --port 6484 --replicaof "localhost 6379" &
pid4=$!

function cleanup {
	kill $pid1
	kill $pid2
	kill $pid3
	kill $pid4
}

trap cleanup EXIT

sleep 1
for i in {1..100}
do
	redis-cli -p 6379 set key$i value$i > /dev/null
done

sleep 2

echo "check 6481"

for i in {1..100}
do
	value=$(redis-cli -p 6481 get key$i)
	if [ "$value" != "value$i" ]; then
		echo "value$i is not value$i"
	fi
done

echo "check 6482"

for i in {1..100}
do
	value=$(redis-cli -p 6482 get key$i)
	if [ "$value" != "value$i" ]; then
		echo "value$i is not value$i"
	fi
done

echo "check 6483"

for i in {1..100}
do
	value=$(redis-cli -p 6483 get key$i)
	if [ "$value" != "value$i" ]; then
		echo "value$i is not value$i"
	fi
done

echo "check 6484"

for i in {1..100}
do
	value=$(redis-cli -p 6484 get key$i)
	if [ "$value" != "value$i" ]; then
		echo "value$i is not value$i"
	fi
done


echo "PASS"

# sleep 1000
