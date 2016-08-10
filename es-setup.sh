#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: es-setup.sh <IP ADDRESS OF ES MASTER >"
    exit 1
fi

MASTER_IPADDR="$1"

sudo apt-get -y install default-jdk wget
wget -qO - https://packages.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
echo "deb http://packages.elastic.co/elasticsearch/2.x/debian stable main" | sudo tee /etc/apt/sources.list.d/elasticsearch-2.x.list
sudo apt-get update
sudo apt-get -y install elasticsearch
sudo sed -i 's/# cluster.name: .*/cluster.name: perf/g' /etc/elasticsearch/elasticsearch.yml
sudo sed -i 's/# network.host: .*/network.host: _eth0_, _local_/g' /etc/elasticsearch/elasticsearch.yml
sudo sed -i 's/# discovery.zen.ping.unicast.hosts: .*/discovery.zen.ping.unicast.hosts: ["'${MASTER_IPADDR}'"]/g' /etc/elasticsearch/elasticsearch.yml
sudo /etc/init.d/elasticsearch start
