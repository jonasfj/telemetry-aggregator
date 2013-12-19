#!/usr/bin/env python

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from boto.sqs import connect_to_region as sqs_connect
from boto.s3 import connect_to_region as s3_connect
from boto.s3.key import Key
from boto.sqs.jsonmessage import JSONMessage
from multiprocessing import Queue, cpu_count
from time import sleep
import os, sys, shutil, gzip
import json
from cStringIO import StringIO
from datetime import datetime
from subprocess import Popen, PIPE

from utils import mkdirp
from s3put import s3put
from s3get import s3get, Downloader
from updateresults import updateresults

"""
Process (v3):
 - Get 100 messages
 - Download payloads in parallel
 - ./mergeresults -f temp-output/ -i ...
 - Get 100 messages
 - Download payloads in parallel
 - ./mergeresults -f temp-output/ -i ...
 - ... (after 6 hours or out of messages for 2 hour)
 - python updateresults.py -f temp-output/ -b <bucket> -p v3/ -c cache/
 - Delete accumulate messages

Features:
 - We can adjust timing
 - Results are out every 6 hours
 - We update at-most every 2nd hour (giving time for s3 propagation)

"""

MESSAGES_TO_MERGE =  10
TIME_TO_PUBLISH = 4 * 60 * 60 #s
MESSAGES_BEFORE_PUBLISH = 100
IDLE_WAIT = 60 * 10
NB_WORKERS = 16
MERGERESULT_PATH = './build/mergeresults'
MESSAGE_BLOCK_SIZE = 10      # max 10 (should be 10)

import commands

class Aggregator:
    def __init__(self, input_queue, work_folder, bucket, prefix, region, aws_cred):
        self.input_queue_name       = input_queue
        self.work_folder            = work_folder
        self.data_folder            = os.path.join(work_folder, 'data')
        self.bucket_name            = bucket
        self.prefix                 = prefix
        self.region                 = region
        self.aws_cred               = aws_cred
        self.analysis_bucket_name   = "dashboard-mango-aggregates"
        if self.prefix != '' and not self.prefix.endswith('/'):
            self.prefix += '/'
        # Clear the work folder
        shutil.rmtree(self.work_folder, ignore_errors = True)
        self.s3 = s3_connect(self.region, **self.aws_cred)
        self.bucket = self.s3.get_bucket(self.bucket_name, validate = False)
        self.analysis_bucket = self.s3.get_bucket(self.analysis_bucket_name,
                                                  validate = False)
        mkdirp(self.data_folder)
        self.cache_folder = os.path.join(self.work_folder, "cache")
        mkdirp(self.cache_folder)
        self.files_missing_path = os.path.join(self.work_folder, 'FILES_MISSING')
        self.files_processed_path = os.path.join(self.work_folder, 'FILES_PROCESSED')
        self.get_file('FILES_PROCESSED', self.files_processed_path)
        self.get_file('FILES_MISSING', self.files_missing_path)

    def get_file(self, prefix, filename):
        try:
            k = Key(self.bucket)
            k.key = self.prefix + prefix
            k.get_contents_to_filename(filename)
        except:
            print >> sys.stderr, "Failed to get: " + filename
            os.system('touch ' + filename)

    def put_file(self, filename, prefix):
        k = Key(self.bucket)
        k.key = self.prefix + prefix
        k.set_contents_from_filename(filename)

    def process_queue(self):
        # Find files exported and not yet merged
        files_exported = []
        with open('files-exported', 'r') as f:
            for line in f:
                files_exported.append(line.strip())
        with open(self.files_processed_path, 'r') as f:
            for line in f:
                if line.strip() not in files_exported:
                    print >> sys.stderr, "File missing from exported: " + line
                    sys.exit(1)
                files_exported.remove(line.strip())

        # messages processed since last flush
        results = []
        last_flush = datetime.utcnow()
        while True:
            print "### Handling messages"

            # get new_messages from sqs
            messages = []
            for i in xrange(0, MESSAGES_TO_MERGE):
                msg = files_exported.pop()
                messages.append(msg)
                results.append(msg)
            print " - Fetched %i messages" % len(messages)
            # merge messages into data folder
            if len(messages) > 0:
                self.merge_messages(messages)
            else:
                if len(results) > 0:
                    self.publish_results()
                print "I think we're done"
                sys.exit(0)
            # Publish if necessary
            publish = False
            # Check disk space available
            dev, b, used, avail, p, f = commands.getoutput("df /mnt | tail -1").split()
            # Publish if using half disk capacity... this is normally bad...
            if float(used) / float(avail) > 0.5:
                publish = True
            if len(results) > MESSAGES_BEFORE_PUBLISH:
                publish = True
            if (datetime.utcnow() - last_flush).seconds > TIME_TO_PUBLISH:
                publish = True
            if publish:
                # Skip publishing if there are no new results
                if len(results) == 0:
                    continue
                self.publish_results()
                results = []
                last_flush = datetime.utcnow()

    def merge_messages(self, results):
        started = datetime.utcnow()
        # Download results
        if len(results) > 0:
            target_paths = []
            download_queue = Queue()
            result_queue = Queue()
            # Make a job queue
            i = 0
            for result in results:
                i += 1
                result_path = os.path.join(self.work_folder, "result-%i.txt" % i)
                download_queue.put((result, result_path))
                target_paths.append(result_path)

            # Start downloaders
            downloaders = []
            for i in xrange(0, 15):
                downloader = Downloader(download_queue, result_queue,
                                        self.analysis_bucket_name, False, False,
                                        self.region, self.aws_cred)
                downloaders.append(downloader)
                downloader.start()
                download_queue.put(None)

            # Wait and process results as they are downloaded
            downloaded_paths = []
            try:
                while len(target_paths) > 0:
                    path = result_queue.get(timeout = 30 * 60)
                    target_paths.remove(path)
                    downloaded_paths.append(path)
            except:
                print >> sys.stderr, "Failed to download an result.txt file"
                sys.exit(1)

            # Check that downloaders downloaded correctly
            for downloader in downloaders:
                downloader.join()
                if downloader.exitcode != 0:
                    sys.exit(1)

            print " - Downloaded results"

            args = ['-f', self.data_folder]
            for path in downloaded_paths:
                args += ['-i', path]
            mergeresults = Popen(
                [MERGERESULT_PATH] + args,
                stdout = sys.stdout,
                stderr = sys.stderr
            )
            mergeresults.wait()
            if mergeresults.returncode != 0:
                print >> sys.stderr, "mergeresults failed exiting non-zero!"
                sys.exit(1)

            # Remove files as they are handled
            for path in downloaded_paths:
                os.remove(path)

            print " - Merged results in %i s" % ((datetime.utcnow() - started).seconds)

        # Update FILES_PROCESSED and FILES_MISSING
        with open(self.files_missing_path, 'a+') as files_missing:
            with open(self.files_processed_path, 'a+') as files_processed:
                for result in results:
                    files_processed.write(result + "\n")

    def publish_results(self):
        # Create work folder for update process
        update_folder = os.path.join(self.work_folder, "update")
        shutil.rmtree(update_folder, ignore_errors = True)
        mkdirp(update_folder)
        # Update results
        updateresults(self.data_folder, update_folder, self.bucket_name,
                      self.prefix, self.cache_folder, self.region,
                      self.aws_cred, NB_WORKERS)
        self.put_file(self.files_processed_path, 'FILES_PROCESSED')
        self.put_file(self.files_missing_path, 'FILES_MISSING')
        # Clear data_folder
        shutil.rmtree(self.data_folder, ignore_errors = True)
        mkdirp(self.data_folder)

def main():
    p = ArgumentParser(
        description = 'Aggregated and upload dashboard results',
        formatter_class = ArgumentDefaultsHelpFormatter
    )
    p.add_argument(
        "input_queue",
        help = "Queue with results from analysis jobs"
    )
    p.add_argument(
        "-k", "--aws-key",
        help = "AWS Key"
    )
    p.add_argument(
        "-s", "--aws-secret-key",
        help = "AWS Secret Key"
    )
    p.add_argument(
        "-w", "--work-folder",
        help = "Folder to store temporary data in",
        required = True
    )
    p.add_argument(
        "-b", "--bucket",
        help = "Bucket to update with data-files",
        required = True
    )
    p.add_argument(
        "-p", "--prefix",
        help = "Prefix in bucket",
        required = False
    )
    p.add_argument(
        "-r", "--region",
        help = "AWS region to connect to",
        default = 'us-west-2'
    )
    cfg = p.parse_args()

    aws_cred = {
        'aws_access_key_id':        cfg.aws_key,
        'aws_secret_access_key':    cfg.aws_secret_key
    }

    aggregator = Aggregator(cfg.input_queue, cfg.work_folder, cfg.bucket,
                            cfg.prefix, cfg.region, aws_cred)
    aggregator.process_queue()


if __name__ == "__main__":
    sys.exit(main())