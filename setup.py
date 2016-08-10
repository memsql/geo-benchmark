#!/usr/bin/env python

import os
import string
import sys
import pickle
import shlex
import subprocess
from optparse import OptionParser
from os.path import expanduser, abspath, dirname, join
import ConfigParser

Config = ConfigParser.ConfigParser()
Config.read('benchmark.cfg')

def parse_args():
    """ Argument parsing. """
    parser = OptionParser()
    parser.add_option("-a", "--aggregator", action="append", dest="aggregators", default=[])
    parser.add_option("-A", "--aggregators-file", 
                        default=[], help='aggregators file to read from',  dest="aggfile")
    parser.add_option("-v", "--verbose", action="store_true", default=False)
    (options, args) = parser.parse_args()
    global VERBOSE
    VERBOSE = options.verbose
    return options

def vprint(args):
    if VERBOSE:
        print(args)
        
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
    
def get_aggregators(options):
    print('Getting hosts from file... %s' % options.aggfile)
    path = join(dirname(abspath(__file__)), options.aggfile)
    with open(path, 'r') as f:
        for line in f:
            options.aggregators.append(line)
    return options.aggregators
    
def scp_files_to_cluster(options):
    
    files = [
        'es-setup.sh',
        'datagen.py',
    ]
    
    print "Copying files around the cluster"
    for aggregator in options.aggregators:
        
        agg_host = aggregator.strip()

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
            
        # Copy files to all 
        for f in files:
            cmd = shlex.split(copy_cmd % expanduser(f))
            subprocess.Popen(cmd, stdout=subprocess.PIPE).wait()

    
def setup(options):
    processes = []
    
    dependencies = [
        # Install dependencies quietly (-q)
        'sudo apt-get install -y python-numpy',
    ]
    
    # Start local setup
    print("Setting up Master aggregator...")
    
    # Install local dependencies
    for dependency in dependencies:
        vprint("Installing: %s" % dependency.rsplit(None, 1)[-1])
        processes.append(subprocess.Popen(shlex.split(dependency)))
        
    # Install remote dependencies
    for aggregator in options.aggregators:
        vprint("Setting up aggregator: %s" % aggregator)
        
        for dependency in dependencies:
        
            remote_cmd = 'nohup %s' % dependency
            ssh_user = benchmark_config("ssh")['username']
            ssh_key = benchmark_config("ssh")['ssh_key']
            if ssh_key and ssh_user:
                ssh_args = '-i %s' % expanduser(ssh_key)
                ssh_args += ' -o StrictHostKeyChecking=no'
                run_cmd = 'ssh %s %s@%s %s' % \
                    (ssh_args, ssh_user, aggregator, remote_cmd)
            elif not ssh_key and ssh_user:
                run_cmd = 'ssh %s@%s %s' % \
                    (ssh_user, aggregator, remote_cmd)
            else:
                run_cmd = 'ssh %s %s' % \
                    (aggregator, remote_cmd)

            vprint("Appending command: %s" % run_cmd)
            processes.append(subprocess.Popen(shlex.split(run_cmd)))

    [p.wait() for p in processes]
        
    scp_files_to_cluster(options)

if __name__ == '__main__':
    options = parse_args()
    if options.aggfile:
        get_aggregators(options)
    setup(options)
    
