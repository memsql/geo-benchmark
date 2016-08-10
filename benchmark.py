#!/usr/bin/env python

import os
import cPickle as pickle
import datagen
import multiprocessing
import shlex
import socket
import subprocess
import sys
import threading
import time
import requests
import ConfigParser

from optparse import OptionParser
from os.path import abspath, expanduser, isfile, dirname, join
from collections import namedtuple

from memsql.common import database

MAX_NUM_WORKERS = 100
VERBOSE = False

Config = ConfigParser.ConfigParser()
Config.read('benchmark.cfg')

if sys.version_info.major == 3:
    xrange = range

Row = namedtuple('Row', ['id', 'latitude', 'longitude'])

def vprint(args):
    if VERBOSE:
        print(args)


class Timer(object):
    def __enter__(self): 
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start



def get_connection(options, db=''):
    """ Returns a new connection to the database. """
    return database.connect(host=options.host, port=options.port,
                            user=options.user, database=db)

def benchmark_config(section):
    config = {}
    options = Config.options(section)
    for option in options:
        try:
            config[option] = Config.get(section, option)
        except:
            print("%s:" % option)
            config[option] = None
    return config


def parse_args():
    """ Argument parsing. """
    parser = OptionParser()
    parser.add_option("-d", "--database", default='perfdb')
    parser.add_option("-t", "--table", default='records')
    parser.add_option("--host", default='127.0.0.1')
    parser.add_option("--user", default='root')
    parser.add_option("-p", "--port", default=3306)
    parser.add_option("--data-file", default=expanduser('~/geo-benchmark/data'),
                      help='data file to read from')
    parser.add_option("--workload-time", default=10)
    parser.add_option("-a", "--aggregator", action="append", dest="aggregators",
                      help=("provide aggregators to run on. if none are "
                            "provided, the script runs locally"),
                      default=[])
    parser.add_option("-A", "--aggregators-file", 
                      default=[], help='aggregators file to read from',  dest="aggfile")
                      
    parser.add_option("--batch-size", default=1000)
    parser.add_option("--num-workers", default=multiprocessing.cpu_count())

    parser.add_option("--no-setup", action="store_true", default=False)
    parser.add_option("--mode", choices=["master", "child"],
                      default="master")
    parser.add_option("-e", "--elastic", dest="use_elastic",
                      action="store_true", default=False)
    parser.add_option("--elastichost", default='127.0.0.1')
    parser.add_option("--elasticport", default=9200)
    parser.add_option("--elasticindex", default='geo_benchmark')
    parser.add_option("--elasticshards", default=8)
    parser.add_option("--elasticreplicas", default=0)
    parser.add_option("--workload-type", choices=["write", "read"], default="write")

    parser.add_option("--drop-database", action="store_true", default=False)
    parser.add_option("--cluster-memory", default=1,  # gigabytes
                      help=("How much total memory the cluster has. The "
                            "number of attempted rows to be inserted is a "
                            "function of this"))
    parser.add_option("-v", "--verbose", action="store_true", default=False)
    (options, args) = parser.parse_args()
    global VERBOSE
    VERBOSE = options.verbose
    try:
        options.workload_time = int(options.workload_time)
    except TypeError:
        sys.stderr.write('workload-time must be an integer')
        exit(1)
    return options


def setup_perf_db(options):
    """ Create a database and table for this benchmark to use. """

    with get_connection(options, db='information_schema') as conn:
        vprint('Creating database %s' % options.database)
        conn.query('create database if not exists %s' % options.database)
        conn.query('use %s' % options.database)
        conn.query('set global multistatement_transactions = 0')

        vprint('Creating table %s' % options.table)

        create_cmd = ('create table if not exists %s ('
                      'id integer primary key,'
                      'location geographypoint,'
                      'key(location))') % options.table

        conn.query(create_cmd)

        conn.query('drop table if exists locations')
        create_ref_cmd = """create reference table if not exists locations (
                            id integer primary key,
                            name varchar(128),
                            polygon geography DEFAULT NULL)"""

        conn.query(create_ref_cmd)

        with open('locations.sql', 'r') as f:
            insert_locations_cmd = f.read()
            conn.query(insert_locations_cmd)

def setup_location_index(options):
    urlprefix = "http://%s:%d/" % (options.elastichost, options.elasticport)
    r = requests.head(urlprefix + options.elasticindex + "/")
    if r.status_code == 200:
        print "Locations index is present"
        return

    settings = '{"settings":{"index":{"number_of_shards": 1,"number_of_replicas":0,"auto_expand_replicas":"0-all"}}}'
    r = requests.put(urlprefix + "locations" + "/", data=settings)
    r.raise_for_status()
    mapping = """{
	"locations": {
		"properties": {
			"name" : {
				"type":"string"
			},
			"polygon": {
				"type": "geo_shape",
				"tree": "quadtree",
				"precision": "1m"
			}
		}
	}
}"""
    r = requests.put(urlprefix + ("locations/_mapping/locations"), data=mapping)
    r.raise_for_status()
    with open('locations.json', 'r') as f:
        locations = f.read()
        r = requests.post(urlprefix + "locations/locations/_bulk", data=locations)
        r.raise_for_status()



def setup_perf_index(options):
    settings = '{"settings":{"index":{"number_of_shards": %d,"number_of_replicas":%d}}}' \
               % (int(options.elasticshards), int(options.elasticreplicas))
    urlprefix = "http://%s:%d/" % (options.elastichost, options.elasticport)
    r = requests.head(urlprefix + options.elasticindex + "/")
    if r.status_code == 200:
        print "Index exists; skipping setup."
        return
    r = requests.put(urlprefix + options.elasticindex + "/", data=settings)
    r.raise_for_status()
    mapping = """{
	"driver": {
	    "dynamic": "strict",
		"properties": {
			"location": {
				"type": "geo_shape",
				"tree": "quadtree",
				"points_only":"true",
				"precision": "1m"
			}
		}
	}
}"""
    r = requests.put(urlprefix + ("%s/_mapping/driver" % options.elasticindex), data=mapping)
    r.raise_for_status()

def setup(options):
    if options.use_elastic:
        setup_location_index(options)
        setup_perf_index(options)
    else:
        setup_perf_db(options)


def get_aggregators(options):
    print('Getting hosts from file... %s' % options.aggfile)
    path = join(dirname(abspath(__file__)), options.aggfile)
    with open(path, 'r') as f:
        for line in f:
            options.aggregators.append(line)
    return options.aggregators
    

def convert_cluster_mem_to_num_rows(options):
    """ Converts the command line arg cluster-memory into
        the number of rows to upsert. cluster-memory is given
        in gigabytes. """
    mem_bytes = int(float(options.cluster_memory) * (1024 ** 3))
    sample_row = Row(id=1234, latitude=42.12343, longitude=72.74237)
    estimated_db_mem = 136  # discussed offline
    cost_per_row = sys.getsizeof(sample_row) + estimated_db_mem
    num_rows = (mem_bytes / cost_per_row) / 2
    num_machines = len(options.aggregators) + 1
    return num_rows / num_machines


def generate_data_file(options):
    num_rows = convert_cluster_mem_to_num_rows(options)
    vprint('Generating test data: {:,} rows'.format(num_rows))
    if isfile(options.data_file):
        vprint('Using existing data file: %s' % options.data_file)
        return
    datagen.main(num_rows)


class Analytics(object):
    def __init__(self):
        self.upsert_counts = [0 for _ in xrange(MAX_NUM_WORKERS)]
        self.latency_totals = [0 for _ in xrange(MAX_NUM_WORKERS)]
        self.latency_mins = [float("infinity") for _ in xrange(MAX_NUM_WORKERS)]
        self.latency_maxs = [0 for _ in xrange(MAX_NUM_WORKERS)]
        self.start_time = time.time()
        self.last_reported_time = time.time()
        self.last_reported_count = 0
        self.num_records = 0
        self.report_frequency = 100

    def record(self, batch_size, thread_id, latency):
        self.upsert_counts[thread_id] += batch_size
        self.latency_totals[thread_id] += latency
        self.latency_mins[thread_id] = min(latency, self.latency_mins[thread_id])
        self.latency_maxs[thread_id] = max(latency, self.latency_maxs[thread_id])
        self.num_records += 1
        if self.num_records % self.report_frequency == 0:
            self.continuous_report()

    def continuous_report(self):
        interval = (time.time() - self.last_reported_time)
        self.last_reported_time = time.time()
        cur_total = sum(self.upsert_counts)
        total = cur_total - self.last_reported_count
        self.last_reported_count = cur_total
        sys.stdout.write('Current upsert throughput: %d rows / s \r' % (total / interval))
        sys.stdout.flush()

    def update_min(self, latency):
        # Min is associative, so taking first element is fine
        # We only care about the min across the cluster anyway
        self.latency_mins[0] = min(latency, self.latency_mins[0])

    def update_max(self, latency):
        self.latency_maxs[0] = max(latency, self.latency_maxs[0])

    def update_totals(self, latency):
        self.latency_totals[0] += latency


ANALYTICS = Analytics()


class InsertWorker(threading.Thread):
    """ A simple thread which inserts generated data in a loop. """

    def __init__(self, stopping, upserts, thread_id, batch_size):
        super(InsertWorker, self).__init__()
        self.stopping = stopping
        self.daemon = True
        self.exception = None
        self.upserts = upserts
        self.num_distinct_queries = len(self.upserts)
        self.options = options
        self.thread_id = thread_id
        self.batch_size = batch_size

    def run(self):
        count = 0
        batch_size = self.batch_size
        with get_connection(options, db=self.options.database) as conn:
            query_idx = 0
            while (not self.stopping.is_set()):
                with Timer() as t:
                    conn.execute(self.upserts[query_idx])
                ANALYTICS.record(batch_size, self.thread_id, t.interval)
                query_idx = (query_idx + 1) % len(self.upserts)
                count += batch_size
        if self.thread_id == 1:
            print('')

class ReadWorker(threading.Thread):
    """ A simple thread which reads generated data in a loop. """

    def __init__(self, stopping, reads, thread_id, batch_size):
        super(ReadWorker, self).__init__()
        self.stopping = stopping
        self.daemon = True
        self.exception = None
        self.reads = reads
        self.num_distinct_queries = len(self.reads)
        self.options = options
        self.thread_id = thread_id
        self.batch_size = batch_size

    def run(self):
        count = 0
        batch_size = self.batch_size
        with get_connection(options, db=self.options.database) as conn:
            query_idx = 0
            while (not self.stopping.is_set()):
                with Timer() as t:
                    conn.execute(self.reads[query_idx])
                ANALYTICS.record(batch_size, self.thread_id, t.interval)
                query_idx = (query_idx + 1) % len(self.reads)
                count += batch_size
        if self.thread_id == 1:
            print('')

def handle_bulk_errors(result):
    if result["errors"] is True:
        # Find the first needle in that haystack...
        items = result["items"]
        error = items[0]
        for item in items:
            res = item["index"]
            if "error" in res:
                error = res["error"]

        raise Exception("Errors occurred during indexing!", error["reason"])

class HttpInsertWorker(threading.Thread):
    """ A simple thread which inserts generated data in a loop. """

    def __init__(self, stopping, upserts, thread_id, batch_size):
        super(HttpInsertWorker, self).__init__()
        self.stopping = stopping
        self.daemon = True
        self.exception = None
        self.upserts = upserts
        self.num_distinct_queries = len(self.upserts)
        self.options = options
        self.thread_id = thread_id
        self.batch_size = batch_size

    def run(self):
        count = 0
        batch_size = self.batch_size

        url = "http://%s:%d/%s/driver/_bulk" % (options.elastichost, options.elasticport, options.elasticindex)

        query_idx = 0
        while (not self.stopping.is_set()):
            with Timer() as t:
                r = requests.post(url, data=self.upserts[query_idx])
                r.raise_for_status()
                handle_bulk_errors(r.json())

            ANALYTICS.record(batch_size, self.thread_id, t.interval)
            query_idx = (query_idx + 1) % len(self.upserts)
            count += batch_size
        if self.thread_id == 1:
            print('')

class HttpReadWorker(threading.Thread):
    """ A simple thread which reads generated data in a loop. """

    def __init__(self, stopping, reads, thread_id, batch_size):
        super(HttpReadWorker, self).__init__()
        self.stopping = stopping
        self.daemon = True
        self.exception = None
        self.reads = reads
        self.num_distinct_queries = len(self.reads)
        self.options = options
        self.thread_id = thread_id
        self.batch_size = batch_size

    def run(self):
        count = 0
        batch_size = self.batch_size

        url = "http://%s:%d/%s/_search" % (options.elastichost, options.elasticport, options.elasticindex)

        query_idx = 0
        while (not self.stopping.is_set()):
            with Timer() as t:
                r = requests.post(url, data=self.reads[query_idx])
                r.raise_for_status()

            ANALYTICS.record(batch_size, self.thread_id, t.interval)
            query_idx = (query_idx + 1) % len(self.reads)
            count += batch_size
        if self.thread_id == 1:
            print('')

def warmup(options):
    vprint('Warming up workload')
    if options.use_elastic: return
    with get_connection(options, db=options.database) as conn:
        conn.execute('show tables;')
        conn.execute('set global multistatement_transactions = 0;')

def bulk_op_prefix(id):
    return '{"index":{"_id":"%d"}}' % id

def get_elasticsearch_queries(options, batch_size):
    rows = []
    with open(options.data_file, 'r') as f:
        print('Deserializing data')
        rows = [bulk_op_prefix(Row(*t).id) + '\n' + format_row_json(Row(*t)) for t in pickle.load(f)]

    batches = ['\n'.join(rows[i:i + batch_size]) for i in xrange(0, len(rows), batch_size)]
    return batches

def get_elasticsearch_read_queries(options):
    url = "http://%s:%d/locations/_search" % (options.elastichost, options.elasticport)
    match_all = '{"query":{"match_all":{}},"size":10000,"fields":[]}'
    r = requests.post(url, data=match_all)
    r.raise_for_status()
    results = r.json()
    hits = results["hits"]["hits"]
    ids = [int(hit["_id"]) for hit in hits]
    ids.sort()
    queries = ["""
    {"query":{
    "bool":{
    "must":{
        "match_all":{}
    },"filter":{
        "geo_shape":{
            "location":{
                "indexed_shape":{
                    "id":%d,
                    "type":"locations",
                    "index":"locations",
                    "path": "polygon"
                }
            }
        }
    }
    }}
    }}
    """ % id for id in ids]
    return queries

def format_row_sql(row):
    return '(%r, "POINT(%f %f)")' % (
        row.id,
        row.latitude,
        row.longitude
    )

def format_row_json(row):
    return '{"location":{"type": "point","coordinates":[%f,%f]}}' % (
        row.longitude, row.latitude
    )

def get_queries(options, batch_size):
    with open(options.data_file, 'r') as f:
        rows = []
        print('Deserializing data')
        rows = [format_row_sql(Row(*row)) for row in pickle.load(f)]

    batches = [rows[i:i+batch_size] for i in xrange(0, len(rows), batch_size)]
    for batch in batches:
        batch.sort()

    prefix = 'insert into %s (id, location) values ' % (options.table)
    postfix = ' on duplicate key update location = values(location)'

    return [prefix + ','.join(batch) + postfix for batch in batches]

def get_read_queries(options):
    with get_connection(options, db='information_schema') as conn:
        conn.query("use %s" % options.database)
        get_location_ids = "select id from locations"

        results = conn.query(get_location_ids)

        prefix = 'select r.id, r.location, l.id from %s r, locations l where ' % options.table

        return [prefix + 'l.id = %s and geography_intersects(r.location, l.polygon)' % r['id'] for r in results]

def on_master_agg(options):
    return options.mode == 'master'

def run_benchmark(options):
    """ Run a set of InsertWorkers and record their performance. """
    batch_size = int(options.batch_size)
    num_workers = int(options.num_workers)
    if num_workers > MAX_NUM_WORKERS:
        num_workers = MAX_NUM_WORKERS

    if options.workload_type == 'write':
        print "Running write workload"
        if options.use_elastic:
            upserts = get_elasticsearch_queries(options, batch_size)
        else:
            upserts = get_queries(options, batch_size)

        stopping = threading.Event()
        if options.use_elastic:
            workers = [HttpInsertWorker(stopping, upserts[i::num_workers], i, batch_size)
                   for i in xrange(num_workers)]
        else:
            workers = [InsertWorker(stopping, upserts[i::num_workers], i, batch_size)
                   for i in xrange(num_workers)]

        print('Launching %d write workers with batch size of %d' % (num_workers, batch_size))

        [worker.start() for worker in workers]
        time.sleep(options.workload_time)

        vprint('Stopping write workload')

        stopping.set()
        [worker.join() for worker in workers]
    else:
        print "Running read workload"
        if options.use_elastic:
            reads = get_elasticsearch_read_queries(options)
        else:
            reads = get_read_queries(options)

        stopping = threading.Event()
        if options.use_elastic:
            workers = [HttpReadWorker(stopping, reads[i::options.num_workers], i, batch_size)
                    for i in xrange(options.num_workers)]
        else:
            workers = [ReadWorker(stopping, reads[i::options.num_workers], i, batch_size)
                    for i in xrange(options.num_workers)]

        print('Launching %d read workers with batch size of %d' % (options.num_workers, batch_size))

        [worker.start() for worker in workers]
        time.sleep(options.workload_time)

        vprint('Stopping read workload')

    stopping.set()
    [worker.join() for worker in workers]

def cleanup(options):
    """ Cleanup the database this benchmark is using. """
    print "Dropping existing benchmark data"
    if not options.use_elastic:
        with get_connection(options, db=options.database) as conn:
            conn.query('drop database %s' % options.database)
    else:
        requests.delete('http://%s:%d/%s/' % (options.elastichost, options.elasticport, options.elasticindex))


def hostport_from_aggregator(options, aggregator):
    if ":" in aggregator:
        h, p = aggregator.split(":")
        return h, int(p)
    else:
        return aggregator, options.port

def scp_myself_to_all_aggs(options):
    print "Copying files to aggregators"
    for aggregator in options.aggregators:
        agg_host, agg_port = hostport_from_aggregator(options, aggregator.strip())

        ssh_user = benchmark_config("ssh")['username']
        ssh_key = benchmark_config("ssh")['ssh_key']
        benchmark_path = os.path.expanduser(os.path.join("~", "geo-benchmark"))
        
        if ssh_user and ssh_key:
            
            uri = '%s@%s' % (ssh_user, agg_host)
            remote_cmd = ':'.join([uri,benchmark_path])
            
            # If you have password-less ssh, you shouldn't need some of these args
            ssh_args = '-i %s' % expanduser(ssh_key)
            ssh_args += ' -o StrictHostKeyChecking=no'
            
            # mkdir on aggregator if it does not exist
            mkdir_cmd = 'ssh %s %s@%s mkdir -p %s' % \
                (ssh_args, ssh_user, agg_host, benchmark_path)
            subprocess.Popen(shlex.split(mkdir_cmd)).wait()
            
            copy_cmd = 'scp %s %%s %s' % (ssh_args, remote_cmd)
            
        elif not ssh_key and ssh_user:
            print "Trying SCP as %s with password-less ssh" % ssh_user
            uri = '%s@%s:%s' % (ssh_user, agg_host, benchmark_path)
            
            # mkdir on aggregator if it does not exist
            mkdir_cmd = 'ssh %s@%s mkdir -p %s' % \
                (ssh_user, agg_host, benchmark_path)
            subprocess.Popen(shlex.split(mkdir_cmd)).wait()
            # setup command to copy files to aggregator
            copy_cmd = 'scp %%s %s' % (uri)
        else:
            # if no username was specified in the config file, try logged in user
            print "Trying SCP as logged in user..."
            username = os.getlogin()
            
            uri = '%s@%s:%s' % \
                (username, agg_host, benchmark_path)
            
            # mkdir on aggregator if it does not exist
            mkdir_cmd = 'ssh %s@%s mkdir -p %s' % \
            (username, agg_host, benchmark_path)
            subprocess.Popen(shlex.split(mkdir_cmd)).wait()
            # setup command to copy files to aggregator
            copy_cmd = 'scp %%s %s' % (uri)

        # Copy python scripts to all aggregators
        
        files_to_copy = [abspath(__file__), abspath(datagen.__file__)]
        if options.workload_type == 'write':
            files_to_copy.append(options.data_file)
        for f in files_to_copy:
            cmd = shlex.split(copy_cmd % expanduser(f))
            subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()


def run_on_all_aggs(options):
    processes = []
    print "Running on aggregators"
    
    for aggregator in ['localhost'] + options.aggregators:
        agg_host, agg_port = hostport_from_aggregator(options, aggregator)
        
        # Get ssh config info
        ssh_user = benchmark_config("ssh")['username']
        ssh_key = benchmark_config("ssh")['ssh_key']

        remote_cmd = 'nohup python %s --mode=child' % abspath(__file__)
        remote_cmd += ' -e' if options.use_elastic else ''
        remote_cmd += ' --database=%s' % options.database
        remote_cmd += ' --table=%s' % options.table
        remote_cmd += ' --port=%s' % agg_port
        remote_cmd += ' --data-file=%s' % options.data_file
        remote_cmd += ' --workload-time=%s' % options.workload_time
        remote_cmd += ' --cluster-memory=%s' % options.cluster_memory
        remote_cmd += ' --workload-type=%s' % options.workload_type

        if ssh_user and ssh_key:
            # If you have password-less ssh, you shouldn't need these
            ssh_args = '-i %s' % expanduser(ssh_key)
            ssh_args += ' -o StrictHostKeyChecking=no'
            user = '%s@' % ssh_user if agg_host != 'localhost' else ''
            run_cmd = 'ssh %s %s%s %s' % \
                (ssh_args, user, agg_host, remote_cmd)
        elif not ssh_key and ssh_user:
            print "Trying with password-less ssh as %s" % ssh_user
            user = '%s@' % ssh_user if agg_host != 'localhost' else ''
            run_cmd = 'ssh %s%s %s' % \
                (user, agg_host, remote_cmd)
        else:
            # if no username was specified in the config file, try logged in user
            print "Trying as logged in user..."
            username = os.getlogin()
            
            user = '%s@' % username if agg_host != 'localhost' else ''
            run_cmd = 'ssh %s%s %s' % \
                (user, agg_host, remote_cmd)
        
        processes.append(subprocess.Popen(shlex.split(run_cmd),
                         stdout=subprocess.PIPE,
                         bufsize=1))
    return processes


def report(options, child_aggs_total=0, avg_latency=None):
    count = sum(ANALYTICS.upsert_counts)
    total_count = count + child_aggs_total
    total_latency = sum(ANALYTICS.latency_totals)
    min_latency = min(ANALYTICS.latency_mins)
    max_latency = max(ANALYTICS.latency_maxs)
    if avg_latency is None and total_count > 0:
        avg_latency = total_latency / float(total_count)

    print('{:,} rows in total'.format(total_count))
    print("{:,} rows per second".format(total_count / options.workload_time))
    if avg_latency is not None:
        print('Average query latency: %.3f ms' % (1000 * avg_latency))
    print('Min query latency: %.3f ms' % (1000 * min_latency))
    print('Max query latency: %.3f ms' % (1000 * max_latency))


def child_agg_report(options):
    count = sum(ANALYTICS.upsert_counts)
    print('%s inserted %s rows' % (socket.gethostname(), count))
    total_latency = sum(ANALYTICS.latency_totals)
    min_latency = min(ANALYTICS.latency_mins)
    max_latency = max(ANALYTICS.latency_maxs)
    if count > 0:
        avg_latency = total_latency / float(count)

    print('{:,} rows in total'.format(count))
    print("{:,} rows per second".format(count / options.workload_time))
    print('Average query latency: %d %f s' % (count, avg_latency))
    print('Min query latency: %f s' % (min_latency))
    print('Max query latency: %f s' % (max_latency))


def average_of_averages(avg_and_counts):
    total_count = float(sum([count for _, count in avg_and_counts]))
    result = 0
    for avg, count in avg_and_counts:
        result += (count / total_count) * avg
    return result


def master_aggregator_main(options):
    try:
        if options.drop_database:
            if options.workload_type == 'read':
                print "Warning: drop-database incompatible with read workload type"
            else:
                cleanup(options)

        if not options.no_setup:
            setup(options)
            warmup(options)
            if options.workload_type == 'write':
                generate_data_file(options)

        if options.aggregators:
            if not options.no_setup:
                vprint('Distributing files to all machines')
                scp_myself_to_all_aggs(options)
            child_aggs_total = 0
            processes = run_on_all_aggs(options)
            time.sleep(5)  # network latency

            def extract_upsert(line):
                return int(line.strip().split(' ')[-4])

            def extract_latency(line):
                return float(line.strip().split(' ')[-2])

            # Read current upsert speed and final statistics
            # from all aggregators
            num_done = 0
            child_total_and_avg = []
            while num_done < len(options.aggregators) + 1:
                cur_upsert_rates = []
                for proc in processes:
                    line = proc.stdout.readline()
                    if line.strip().endswith('rows / s'):
                        cur_upsert_rates.append(extract_upsert(line))
                    if line.strip().endswith('rows') and 'inserted' in line:
                        child_aggs_total += int(line.split(' ')[-2])
                    if 'Min query latency' in line:
                        ANALYTICS.update_min(extract_latency(line))
                    if 'Max query latency' in line:
                        ANALYTICS.update_max(extract_latency(line))
                        num_done += 1
                    if 'Average query latency' in line:
                        child_avg = extract_latency(line)
                        child_count = int(line.strip().split(' ')[-3])
                        child_total_and_avg.append((child_avg, child_count))
                        pass
                if sum(cur_upsert_rates) > 0:
                    sys.stdout.write('Current upsert: {:,} rows per sec\r'.format(sum(cur_upsert_rates)))
            print('')

            [p.wait() for p in processes]
            report(options, child_aggs_total=child_aggs_total,
                   avg_latency=average_of_averages(child_total_and_avg))
        else:
            run_benchmark(options)
            report(options)
    except KeyboardInterrupt:
        print("Interrupted... exiting...")


def child_aggregator_main(options):
    if not options.no_setup:
        generate_data_file(options)
        warmup(options)
    run_benchmark(options)
    child_agg_report(options)


if __name__ == '__main__':
    options = parse_args()
    if options.aggfile:
        get_aggregators(options)
        
    if "master" == options.mode:
        master_aggregator_main(options)
    elif "child" == options.mode:
        child_aggregator_main(options)
    else:
        exit(1)
