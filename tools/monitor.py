#!/usr/bin/python

# script to monitor an application using the gateway REST API. Run:
#     python monitor.py --help
# for more info on how to run it
#

import argparse, getopt, json, logging, logging.handlers, os.path, requests, sys, time
from sys import argv

class Monitor:
    TIMEOUT = 5    # seconds for HTTP requests to timeout

    # LOG_FORMAT -- format of log messages
    # LOG_SIZE   -- max size of a log file in bytes
    # LOG_COUNT  -- max number of log file
    #
    LOG_FORMAT = '%(asctime)s %(name)s %(levelname)s - %(message)s'
    LOG_SIZE = 2**20
    LOG_COUNT = 3

    def __init__(self):
        self.args = None    # parsed commandline arguments

    def getAbout(self):
        '''Get basic gateway info'''

        path = 'ws/v2/about'
        url = 'http://{0}:{1}/{2}'.format(self.args.gateway, self.args.port, path)

        headers={'Accept': 'application/json'}
        try:
            response = requests.get(url=url, headers=headers, timeout=self.TIMEOUT)
            result = json.dumps(response.json(), indent=4)
            self.logger.info(result)
        except requests.exceptions.Timeout:
            self.logger.error('getAbout: request timed out')

    def getApps(self):
        '''Get list of applications'''

        path = 'ws/v2/applications'
        url = 'http://{0}:{1}/{2}'.format(self.args.gateway, self.args.port, path)

        headers={'Accept': 'application/json'}
        try:
            response = requests.get(url=url, headers=headers, timeout=self.TIMEOUT)
            self.result = response.json()
            pretty = json.dumps(self.result, indent=4)
            self.logger.debug(pretty)
        except requests.exceptions.Timeout:
            self.result = None
            msg = 'Request to gateway timed out'
            self.logger.error(msg)
            self.alert(msg)

    def findApp(self):
        '''Find the list of records for the given application name; assumes that
        getApps has already populated self.result with a JSON object of the
        form {"apps": [{...}, {...}, ...]}
        '''
        if not self.result:
            self.logger.warn('self.result not set')
            return

        # filter application list be name and status
        apps = self.result['apps']
        name = self.args.app_name
        list = [x for x in apps if name == x['name'] and 'RUNNING' == x['state']]
        if not list:
            msg = 'No RUNNING applications found with name "{0}"'.format(name)
            self.alert(msg)
            return

        size = len(list)
        self.logger.debug('Found {0} apps with name "{1}"'.format(size, name))
        if len(list) > 1:
            msg = '{0} applications found with name "{1}"'.format(len(list), name)
            self.alert(msg)
            return

        # all is well
        app = list[0]
        self.logger.debug('id = {0}, state = {1}'.format(app['id'], app['state']))

    def init_logger(self):
        ''' initialize logger based on parsed options '''

        self.logger = logging.getLogger(__name__)

        if self.args.quiet:
            self.logger.addHandler(logging.NullHandler)
        else:
            formatter = logging.Formatter(self.LOG_FORMAT)
            path = self.args.log_file
            fh = logging.handlers.RotatingFileHandler(path, maxBytes=self.LOG_SIZE,
                                                      backupCount=self.LOG_COUNT)
            fh.setFormatter(formatter)
            level = logging.DEBUG if self.args.debug else logging.INFO
            # fh.setLevel(level)    # not needed
            self.logger.addHandler(fh)
            self.logger.setLevel(level)

            # log arguments
            a = self.args
            fmt = ('gateway = {0}, port = {1}, app_name = {2}, interval = {3}, '
                   'log_file = {4}, debug = {5}')
            msg = fmt.format(a.gateway, a.port, a.app_name, a.interval,
                             a.log_file, a.debug)
            self.logger.info(msg)

    def check(self):
        '''Sanity checks'''

        self.args.app_name = self.args.app_name.strip()
        if not self.args.app_name:
            raise ValueError('Error: blank app_name')
        self.args.gateway = self.args.gateway.strip()
        if not self.args.gateway:
            raise ValueError('Error: blank gateway host name')

        # Need path to log file if -q is absent
        if not self.args.quiet:
            if not self.args.log_file:
                raise ValueError('Need path to log file (or quiet option)')

            # have path to log file
            self.args.log_file = self.args.log_file.strip()
            if not self.args.log_file:
                raise ValueError('Path to log file is blank')

    def parse_args(self):
        '''Parse commandline arguments'''
        parser = argparse.ArgumentParser()

        # boolean options
        group = parser.add_mutually_exclusive_group()
        group.add_argument('-d', '--debug', help='Enable debug logging',
                           action='store_true')
        group.add_argument('-q', '--quiet', help='Suppress logging',
                           action='store_true')

        # options with arguments 
        parser.add_argument('-a', '--app-name', help='Application name', required=True)
        parser.add_argument('-g', '--gateway', help='Gateway host name', required=True)
        parser.add_argument('-f', '--log-file', help='Path to log file')
        parser.add_argument('-p', '--port', help='Gateway port [9090]', default=9090)
        parser.add_argument('-i', '--interval',
                            help='Time interval (seconds) between status checks [60]',
                            type=int, default=60)

        self.args = parser.parse_args();

    def alert(self, msg):
        '''Simple alerter: just prints a WARNING to stdout and log file. Enhance as needed
        '''
        print msg
        self.logger.warn(msg)

    def monitor(self):
        '''Enter monitoring loop'''

        sleepTime = self.args.interval
        while True:
            self.getApps()
            self.findApp()
            self.logger.debug('sleeping')
            time.sleep(sleepTime);

    def go(self):
        '''Main entry point'''

        self.parse_args()         # parse commandline arguments
        self.check()              # sanity checks
        self.init_logger()        # initialize logger
        self.getAbout()           # log basic gateway info
        self.monitor()            # enter monitoring loop

if __name__ == "__main__":
    monitor = Monitor()
    monitor.go()
