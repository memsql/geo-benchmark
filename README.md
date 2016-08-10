# A Geospatial Benchmark Script for MemSQL and Elasticsearch

A simple benchmark to help compare performance of MemSQL and Elasticsearch on a Geospatial workload.

## Getting started

Clone or download the repo onto the Master aggregator. You will need to rename the directory to geo-benchmark.

```
git clone https://github.com/memsql/geo-benchmark.git ~/geo-benchmark
```

The script only requires a few dependencies:

* `python`
* `pip`
* `python-numpy`
* `requests`

Note when installing manually, you must manually install all dependencies on the master and all child aggregators.

To install `numpy` and `requests` manually, run

```
sudo apt-get install python-numpy
sudo pip install requests
```

Alternatively, you can run the setup.py command from the master aggregator. 

Note that the script will SSH itself over to the child aggregators and install dependencies, so you will need appropriate ssh and sudo permissions.

If using private a key to ssh, please specify username and key file location in the `benchmark.cfg` file. If credentials are not provided, the script will attempt password-less ssh as logged in user. If using password-less ssh as user other than logged user, specify only the username in the benchmark.cfg file. 

```
$ vi ~/benchmark/benchmark.cfg
```

```
[ssh]
username: admin
ssh_key: ~/.ssh/private.pem
```
Now, we can run the setup scripts.

```
python ./setup.py --aggregator='hostname1' --aggregator='hostname2'
```
where each hostname is the hostname of some child aggregator in your cluster.

Alternatively, you can specify a filename which contains a list of hosts.

```
python ./setup.py -A filename
```

./es_setup.sh `hostname`

## Usage

Ensure benchmark.py is executable.

```
chmod +x benchmark.py
```

### Local 

To run benchmark against a single MemSQL instance.

```
./benchmark.py
```

Provide the -e or --elastic flag to run against Elasticsearch. 
 
```
./benchmark.py -e
```
 
The --cluster-memory and --workload-time flags may also be of interest. The benchmark will run over the generated dataset multiple times, if need be. The number of rows that are attempted to be added is a function of the cluster-memory flag.

### Distributed

```
./benchmark.py --aggregator=hostname1 --aggregator=hostname2
```

The script expects to be run on the master aggregator. If there are child aggregators that you also want to utilize for upserts, you can provide their hostnames via the command line. (`--aggregator=hostname`). You must repeat this flag for each child aggregator you'd like to use.

Note that the script will SCP itself over to the child aggregators, so you will need appropriate ssh permissions.

Again, the `-A` flag can be used to specify a list of hosts that you wish to run the benchmark on.

```
./benchmark.py -A filename
```

For additional information on the other flags available to the script, run

### Help
```
./benchmark.py --help
```

to see the help message.

