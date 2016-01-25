#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c20671.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Runner for MODIS level-1 processing using SeaDAS-7.2. Needs the gbad software
from DRL as well to generate attitude and ephemeris data for Aqua.

"""

import os
import ConfigParser
import logging
LOG = logging.getLogger(__name__)

import sys
from urlparse import urlparse
import posttroll.subscriber
from posttroll.publisher import Publish
import socket
from trollduction import get_local_ips
from datetime import datetime
from multiprocessing import Pool, Manager
import threading
from Queue import Empty

EOS_SATELLITES = ['EOS-Terra', 'EOS-Aqua']
TERRA = 'EOS-Terra'
AQUA = 'EOS-Aqua'

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

SEADAS_HOME = os.environ.get("SEADAS_HOME", '')
APPL_HOME = os.environ.get('MODIS_LVL1PROC', '')
ETC_DIR = os.path.join(SEADAS_HOME, 'ocssw/run/var/modis')

NAVIGATION_HELPER_FILES = ['utcpole.dat', 'leapsec.dat']

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

PACKETFILE_AQUA_PRFX = "P154095715409581540959"
MODISFILE_AQUA_PRFX = "P1540064AAAAAAAAAAAAAA"
MODISFILE_TERRA_PRFX = "P0420064AAAAAAAAAAAAAA"


class FilePublisher(threading.Thread):

    """A publisher for the MODIS level-1 files. Picks up the return value
    from XXX when ready, and publish the files via posttroll

    """

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish('modis_dr_runner', 0, ['EOS/1B', ]) as publisher:

            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)


class FileListener(threading.Thread):

    """A file listener class, to listen for incoming messages with a 
    relevant file for further processing"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with posttroll.subscriber.Subscribe('receiver', ['PDS/0', ], True) as subscr:

            for msg in subscr.recv(timeout=90):
                if not self.loop:
                    break

                # Check if it is a relevant message:
                if self.check_message(msg):
                    LOG.debug("Put the message on the queue...")
                    self.queue.put(msg)

    def check_message(self, msg):
        if not msg:
            return False

        urlobj = urlparse(msg.data['uri'])
        server = urlobj.netloc
        url_ip = socket.gethostbyname(urlobj.netloc)
        if server and (url_ip not in get_local_ips()):
            LOG.warning("Server %s not the current one: %s", str(server),
                        socket.gethostname())
            return False

        if ('platform_name' not in msg.data or
                'orbit_number' not in msg.data or
                'start_time' not in msg.data):
            LOG.info(
                "Message is lacking crucial fields...")
            return False

        if msg.data['platform_name'] not in EOS_SATELLITES:
            LOG.info(str(msg.data['platform_name']) + ": " +
                     "Not an EOS satellite. Continue...")
            return False

        sensor = msg.data.get('sensor', None)
        if sensor not in ['modis']:
            LOG.debug("Not MODIS data, skip it...")
            return False

        LOG.debug("Ok: message = %s", str(msg))
        return True


def modis_live_runner():
    """Listens and triggers processing"""

    LOG.info("*** Start the runner for the MODIS level-1 processing")
    LOG.debug("os.environ = " + str(os.environ))

    pool = Pool(processes=6, maxtasksperchild=1)
    manager = Manager()
    listener_q = manager.Queue()
    publisher_q = manager.Queue()

    pub_thread = FilePublisher(publisher_q)
    pub_thread.start()
    listen_thread = FileListener(listener_q)
    listen_thread.start()

    eos_files = {}
    jobs_dict = {}
    while True:

        try:
            msg = listener_q.get()
        except Empty:
            LOG.debug("Empty listener queue...")
            continue

        LOG.debug(
            "Number of threads currently alive: " + str(threading.active_count()))

        LOG.info("EOS files: " + str(eos_files))
        LOG.debug("\tMessage:")
        LOG.debug(msg)

        if 'start_time' in msg.data:
            start_time = msg.data['start_time']
        else:
            LOG.warning("start_time not in message!")
            start_time = None

        if 'end_time' in msg.data:
            end_time = msg.data['end_time']
        else:
            LOG.warning("No end_time in message!")
            end_time = None

        platform_name = msg.data['platform_name']
        orbit_number = int(msg.data['orbit_number'])
        urlobj = urlparse(msg.data['uri'])
        sensor = msg.data.get('sensor', None)

        keyname = (str(platform_name) + '_' +
                   str(orbit_number) + '_' +
                   str(start_time.strftime('%Y%m%d%H%M')))
        # Check if we have all the files before processing can start:

        status = ready2run(msg, eos_files, jobs_dict, keyname)
        if status:
            # Run
            LOG.info("Ready to run...")
            urlobj = urlparse(msg.data['uri'])
            path, fname = os.path.split(urlobj.path)
            LOG.debug("path " + str(path) + " filename = " + str(fname))

            scene = {'platform_name': platform_name,
                     'orbit_number': orbit_number,
                     'starttime': start_time, 'endtime': end_time,
                     'sensor': sensor,
                     'pdsfilename': urlobj.path}

            if platform_name == TERRA:
                pool.apply_async(run_terra_l0l1,
                                 (scene,
                                  jobs_dict[
                                      keyname],
                                  publisher_q))

    pool.close()
    pool.join()

    pub_thread.stop()
    listen_thread.stop()


def ready2run(message, eosfiles, job_register, sceneid):
    """Check if we have got all the input lvl0 files and that we are
    ready to process MODIS lvl1 data.

    """

    LOG.debug("Scene identifier = " + str(sceneid))
    LOG.debug("Job register = " + str(job_register))
    if sceneid in job_register and job_register[sceneid]:
        LOG.debug("Processing of scene " + str(sceneid) +
                  " have already been launched...")
        return False

    if sceneid not in eosfiles:
        eosfiles[sceneid] = []

    urlobj = urlparse(message.data['uri'])

    if 'start_time' in message.data:
        start_time = message.data['start_time']
    else:
        LOG.warning("start_time not in message!")
        start_time = None

    if (message.data['platform_name'] == "EOS-Terra" and
            message.data['sensor'] == 'modis'):
        # orbnum = message.data.get('orbit_number', None)

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))
        if fname.startswith(MODISFILE_TERRA_PRFX) and fname.endswith('001.PDS'):
            # Check if the file exists:
            if not os.path.exists(urlobj.path):
                LOG.warning("File is reported to be dispatched " +
                            "but is not there! File = " +
                            urlobj.path)
                return False

            eosfiles[sceneid].append(urlobj.path)

            # Do processing:
            LOG.info("Level-0 to lvl1 processing on terra start!" +
                     " Start time = " + str(start_time))

            # # Start checking and dowloading the luts (utcpole.dat and
            # # leapsec.dat):
            # LOG.info("Checking the modis luts and updating " +
            #          "from internet if necessary!")
            # fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
            # if fresh:
            #     LOG.info(
            #         "Files in etc dir are fresh! No url downloading....")
            # else:
            #     LOG.warning("Files in etc are non existent or too old. " +
            #                 "Start url fetch...")
            #     update_utcpole_and_leapsec_files()

    LOG.info("Files ready for MODIS level-1 runner: " +
             str(eosfiles[sceneid]))

    job_register[sceneid] = datetime.utcnow()
    return True


def get_working_dir():
    working_dir = OPTIONS['working_dir']
    if not os.path.exists(working_dir):
        try:
            os.makedirs(working_dir)
        except OSError:
            LOG.error("Failed creating working directory %s", working_dir)
            working_dir = '/tmp'
            LOG.info("Will use /tmp")

    return working_dir


def run_terra_l0l1(scene, job_id, publish_q):
    """Process Terra MODIS level 0 PDS data to level 1a/1b"""

    from subprocess import Popen, PIPE, STDOUT
    from glob import glob

    working_dir = get_working_dir()

    #fdwork = os.open(working_dir, os.O_RDONLY)
    # os.fchdir(fdwork)

    level1b_home = OPTIONS['level1b_home']
    filetype_terra = OPTIONS['filetype_terra']
    geofile_terra = OPTIONS['geofile_terra']
    level1a_terra = OPTIONS['level1a_terra']
    level1b_terra = OPTIONS['level1b_terra']
    level1b_250m_terra = OPTIONS['level1b_250m_terra']
    level1b_500m_terra = OPTIONS['level1b_500m_terra']

    # Get the observation time from the filename as a datetime object:
    bname = os.path.basename(scene['pdsfile'])
    obstime = datetime.strptime(bname, filetype_terra)

    # level1_home
    proctime = datetime.now()
    lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
    firstpart = obstime.strftime(level1b_terra)
    mod021km_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(level1b_250m_terra)
    mod02qkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(level1b_500m_terra)
    mod02hkm_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    lastpart = proctime.strftime("%Y%j%H%M%S.hdf")
    firstpart = obstime.strftime(level1a_terra)
    mod01_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)
    firstpart = obstime.strftime(geofile_terra)
    mod03_file = "%s/%s_%s" % (level1b_home, firstpart, lastpart)

    retv = {'mod021km_file': mod021km_file,
            'mod02hkm_file': mod02hkm_file,
            'mod02qkm_file': mod02qkm_file,
            'level1a_file': mod01_file,
            'geo_file': mod03_file}

    mod01files = glob("%s/%s*hdf" % (level1b_home, firstpart))
    if len(mod01files) > 0:
        LOG.warning(
            "Level 1 file for this scene already exists: %s", mod01files[0])

    LOG.info("Level-1 filename: " + str(mod01_file))
    modisl1_home = os.path.join(SEADAS_HOME, "ocssw/run/scripts")
    cmdl = ["%s/modis_L1A.py" % modisl1_home,
            "--verbose",
            "--mission=T",
            "--startnudge=5",
            "--stopnudge=5",
            scene['pdsfile']]

    LOG.debug("Run command: " + str(cmdl))


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-file",
                        required=True,
                        dest="config_file",
                        type=str,
                        default=None,
                        help="The file containing configuration parameters.")
    parser.add_argument("-l", "--log-file", dest="log",
                        type=str,
                        default=None,
                        help="The file to log to (stdout per default).")

    args = parser.parse_args()

    CONF = ConfigParser.ConfigParser()

    print "Read config from", args.config_file

    CONF.read(args.config_file)

    OPTIONS = {}
    for option, value in CONF.items(MODE, raw=True):
        OPTIONS[option] = value

    DAYS_BETWEEN_URL_DOWNLOAD = OPTIONS.get('days_between_url_download', 14)
    DAYS_KEEP_OLD_ETC_FILES = OPTIONS.get('days_keep_old_etc_files', 60)
    URL = OPTIONS['url_modis_navigation']

    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('seadas_modis_runner')

    modis_live_runner()
