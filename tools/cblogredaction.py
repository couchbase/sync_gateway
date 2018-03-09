#!/usr/bin/env python
# -*-python-*-

"""
Couchbase log redaction tool. This tool is used to redact Couchbase SDK and
tool log files.
"""

import argparse
import hashlib
import logging
import os
import random
import re
import signal
import sys
import time
from multiprocessing import Pool

UD_TAG_REGEX = re.compile('(<ud>)(.+?)(</ud>)')


class Redact_file(object):
    def __init__(self, log, salt, path):
        self.log = log
        self.salt = salt
        self.path = path
        self._redact_file()

    def _redact_tag(self, match):
        """
        Takes a regex match of the tag and returns a string with the data
        between the tags has been redacted.
        """
        hash_object = hashlib.sha1(self.salt + str(match.group(2)))
        return match.group(1) + hash_object.hexdigest() + match.group(3)

    def _redact_line(self, logline):
        """Takes a log line and return a redacted line"""
        result = UD_TAG_REGEX.sub(self._redact_tag, logline)
        return result

    def _redact_file(self):
        """
        Reads a log file and creates a new file called "redacted-[FILENAME]".
        Where data between tags have been redacted. It will report warnings
        if there are not matching tags on a line.
        """
        try:
            with open(self.log, 'r') as log_file:
                try:
                    total_redacted_tags = 0
                    warning_lines = 0
                    _, tail = os.path.split(self.log)
                    redacted_log = os.path.join(self.path, 'redacted-' + tail)
                    if os.path.exists(redacted_log):
                        logging.error('{} - {} already exists, not redacting'.format(self.log, redacted_log))
                        return
                    logging.debug('{} - Starting redaction file size is {}'
                                  ' bytes'.format(self.log, os.fstat(log_file.fileno()).st_size))
                    with open(redacted_log, 'w+') as redacted_file:
                        hashed_salt = hashlib.sha1(self.salt + self.salt).hexdigest()
                        redacted_file.write('Hash of the salt used to redact the file: {}\n'.format(hashed_salt))
                        logging.debug('{} - Log redacted using salt: <ud>{}</ud>'.format(self.log, self.salt))
                        for line_number, line in enumerate(log_file, 1):
                            ud_start_tags = line.count('<ud>')
                            ud_end_tags = line.count('</ud>')
                            if ud_start_tags == ud_end_tags:
                                total_redacted_tags += ud_start_tags
                            else:
                                logging.warn('{} - Unmatched tags detected on line {}, potential data leak. Please'
                                             ' review redacted file'.format(self.log, line_number))
                                if 0 not in [ud_start_tags, ud_end_tags]:
                                    total_redacted_tags += min(ud_start_tags, ud_end_tags)
                                    warning_lines += 1
                            redacted_file.write(self._redact_line(line))
                    logging.info('{} - Finished redacting, {} lines processed, {} tags redacted, {} lines with'
                                 ' unmatched tags'.format(self.log, line_number, total_redacted_tags, warning_lines))
                except IOError as e:
                    logging.error('{} - {}'.format(redacted_log, e.strerror))
                except Exception, e:
                    # Should not get here in a production environment, but this is useful for development
                    logging.error('{} - Unexpected error: {}'.format(self.log, e.message))
        except IOError as e:
            logging.error('{} - {}'.format(self.log, e.strerror))


def redact_file_unpack(args):
    """
    Helper fuction to unpack arguments for the Redact_file class as Python2.6
    does not support passing mutliple arguments to multiprocessing.Pool
    """
    return Redact_file(*args)


def _init_worker_singal():
    """Initialisations the worker signal handle to allow the capturing of control+c"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _main():
    opts = argparse.ArgumentParser(description='A tool to redact log files outside of Couchbase Server such as, SDK '
                                               'and cbbackupmgr log files. The redacted file will be named '
                                               'redacted-[filename] and will be placed in the current working '
                                               'directory.')
    opts.add_argument('log_files', nargs='+', metavar='File', help='path to the log file(s) to redact')
    salt_group = opts.add_mutually_exclusive_group(required=True)
    salt_group.add_argument('-s', '--salt', type=str, metavar='<string>', help='the salt used to redact the logs')
    salt_group.add_argument('-g', '--generate-salt', action='store_true', dest='generate_salt',
                            help='automatically generates a salt that will be used to redact the logs')
    opts.add_argument('-t', '--threads', type=int, metavar='<num>', default=1,
                      help='number of concurrent workers threads to use')
    opts.add_argument('-o', '--output-dir', type=str, metavar='<path>', dest='output_dir', default='',
                      help='the directory to place the redacted logs in')
    opts.add_argument('-v', '--verbose', action='count', help='increase output verbosity')
    args = opts.parse_args()

    log_level = None
    if args.verbose >= 2:
        log_level = logging.DEBUG
    elif args.verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARN
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y/%m/%dT%H:%M:%S', level=log_level)

    # Unique list in case the same file is passed in twice
    args.log_files = set(args.log_files)

    if args.generate_salt:
        logging.warn('Automatically generating salt. This will make it difficult to cross reference logs')
        ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        args.salt = ''.join(random.choice(ALPHABET) for _ in range(16))

    if not 1 <= args.threads <= 64:
        opts.error('--threads has to be between 1 and 64')

    pool = Pool(args.threads, _init_worker_singal)
    # Creating a list of tuples because of the limitation of pool with multiple arguments
    worker_args = [(log, args.salt, args.output_dir) for log in args.log_files]
    results = pool.map_async(redact_file_unpack, worker_args)
    try:
        while not results.ready():
            time.sleep(1)
    except KeyboardInterrupt:
        pool.terminate()
        sys.exit('Controlled-C, exiting....')
    else:
        pool.close()
    finally:
        pool.join()


if __name__ == '__main__':
    _main()
