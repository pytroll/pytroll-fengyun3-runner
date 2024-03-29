#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 - 2019,2021 PyTroll

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>
#   Trygve Aspenes <trygveas@met.no>

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
"""Runner for FENGYUN 3 level-1 processing using CMS software. 
Needs the 1le data as well as leapsec and utcpole.

The processing is based on the csh script following the CMA processing
software. These scripts assumes some data directories and environmental
variables.
HOME=<path to where fy3 data directory start, a base dir>
$HOME/fy3dl0db/data/org CADU data
$HOME/fy3dl0db/data/mersi_l0 the unpacked mersi data
$HOME/fy3dl0db/data/vass_l0 the unpacked vass data
$HOME/fy3dl0db/data/hiras_l0 the unpacked hiras data
$HOME/fy3dlldb/mersi_l0 the unpacked mersi data, strange, but this is how it works. Usually a link 
$HOME/fy3dlldb/hiras_l0 the unpacked hiras data, see above, only FY3D+
$HOME/fy3dlldb/mwhs_l0 the unpacked mwhs data, see above
$HOME/fy3dlldb/mwts_l0 the unpacked mwts data, see above
$HOME/fy3dlldb/mwri_l0 the unpacked mwri data, see above, only FY3ABC
$HOME/fy3dlldb/mersi_l1 the l1b mersi data
$HOME/fy3dlldb/hiras_l1 the l1b hiras data
$HOME/fy3dlldb/mwhs_l1 the l1b mwhs data
$HOME/fy3dlldb/mwts_l1 the l1b mwts data
$HOME/fy3dlldb/mwri_l1 the l1b mwri data

$HOME/fy3dlldb/gps special gps files need by some of the processing steps. generated by the unpack of vass

$HOME/fy3dl1db/SysData/fy3d1line.dat one line element file, one for each satellite
$HOME/fy3dl1db/SysData/leapsec.dat
$HOME/fy3dl1db/SysData/utcpole.dat
"""

import logging
LOG = logging.getLogger(__name__)

import os
import sys
import shutil
import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message
import socket
from fengyun3_runner.helper_functions import get_local_ips
from datetime import datetime
from multiprocessing import Pool, Manager
import threading
import six
import yaml

if six.PY2:
    from urlparse import urlparse
    from urlparse import urlunsplit
    from Queue import Empty
elif six.PY3:
    from urllib.parse import urlparse
    from urllib.parse import urlunsplit
    from queue import Empty

FY3_SATELLITES = ['Fengyun-3D', 'fengyun 3d']

MWRI_MISSIONS = ['FY3A', 'FY3B', 'FY3C']
HIRAS_MISSIONS = ['FY3D']

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

#FENGYUN3_HOME = os.environ.get("FENGYUN3_HOME", '')
NAVIGATION_HELPER_FILES = ['utcpole.dat', 'leapsec.dat']

MODE = os.getenv("SMHI_MODE")
if MODE is None:
    MODE = "offline"

def reset_job_registry(objdict, fy3files, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Job registry didn't contain any entry matching: " +
                    str(key))

    LOG.debug("Release/reset key " + str(key) + " from fengyun3 files registry")
    if key in fy3files:
        fy3files.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Fengyun-files registry didn't contain any entry matching: " +
                    str(key))
    return


class FilePublisher(threading.Thread):
    """A publisher for the FENGYUN3 level-1 files. Picks up the return value
    from XXX when ready, and publish the files via posttroll

    """

    def __init__(self, queue, publish_topic, nameservers):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.jobs = {}
        self.publish_topic = publish_topic
        self.nameservers = nameservers

    def stop(self):
        """Stops the file publisher"""
        self.loop = False
        self.queue.put(None)

    def run(self):

        with Publish('fengyun3_dr_runner', 0, [self.publish_topic,],
                     nameservers=self.nameservers) as publisher:
            while self.loop:
                retv = self.queue.get()

                if retv != None:
                    LOG.info("Publish the files...")
                    publisher.send(retv)


class FileListener(threading.Thread):
    """A file listener class, to listen for incoming messages with a
    relevant file for further processing"""

    def __init__(self, queue, listen_topic, listen_service=""):
        threading.Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.listen_topic = listen_topic
        self.listen_service = listen_service

    def stop(self):
        """Stops the file listener"""
        self.loop = False
        self.queue.put(None)

    def run(self):
        with posttroll.subscriber.Subscribe(self.listen_service, [
                self.listen_topic,
        ], True) as subscr:

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

        if msg.type != 'file':
            return False

        urlobj = urlparse(msg.data['uri'])
        server = urlobj.netloc
        url_ip = socket.gethostbyname(urlobj.netloc)
        if server and (url_ip not in get_local_ips()):
            LOG.warning("Server %s not the current one: %s", str(server),
                        socket.gethostname())
            return False

        if ('platform_name' not in msg.data or 'orbit_number' not in msg.data
                or 'start_time' not in msg.data):
            LOG.info("Message is lacking crucial fields...")
            return False

        if msg.data['platform_name'] not in FY3_SATELLITES:
            LOG.info(
                str(msg.data['platform_name']) + ": " +
                "Not an FENGYUN3 satellite. Continue...")
            return False

        sensor = msg.data.get('sensor', None)
        print(sensor)
        if 'mersi' not in sensor or 'vass' not in sensor or 'hiras' not in sensor:
            LOG.debug("Not mersi or vass or hiras data, skip it...")
            return False

        LOG.debug("Ok: message = %s", str(msg))
        return True

def check_directories(options):
    def _check_dir(directory):
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
                LOG.debug("Created directory: %s",directory)
            except OSError:
                LOG.error("Failed creating directory %s", directory)
                raise
            
    #_check_dir(options['fy3dl0db'])
    #_check_dir(options['fy3dl0db_gps'])
    #for instr in options['process_instrument_scripts_l1']:
    #    _check_dir(instr['fy3dl0db'])
    #    _check_dir(instr['fy3dl1db_l0'])
    #    _check_dir(instr['fy3dl1db_l1'])


def fengyun3_live_runner(options):
    """Listens and triggers processing"""

    LOG.info("*** Start the runner for the FENGYUN3 level-1 processing")
    LOG.debug("os.environ = " + str(os.environ))

    check_directories(options)

    # Start checking and downloading the luts (utcpole.dat and
    # leapsec.dat):
    LOG.info("Checking the utcpole and leapsec and updating " +
             "from internet if necessary!")
    fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
    if fresh:
        LOG.info("Files in etc dir are fresh! No url downloading....")
    else:
        LOG.warning("Files in etc are non existent or too old. " +
                    "Start url fetch...")
        update_utcpole_and_leapsec_files()

    fresh_1le = check_1le_files(options)
    if fresh_1le:
        LOG.info("1le files in etc dir are fresh! No url downloading....")
    else:
        LOG.warning("1le files in etc are non existent or too old. " +
                    "Start url fetch...")
        # Need to download the 1le file
        download_1le(options)

    pool = Pool(processes=6, maxtasksperchild=1)
    manager = Manager()
    listener_q = manager.Queue()
    publisher_q = manager.Queue()
    # Assume nameservers is a list
    nameservers = options.get('nameservers', None)
    pub_thread = FilePublisher(publisher_q, options.get('publish_topic'),
                               nameservers=nameservers)
    pub_thread.start()
    listen_thread = FileListener(listener_q,
                                 options.get('listen_topic','/PDS/0'),
                                 options.get('listen_service'))
    listen_thread.start()

    fy3_files = {}
    jobs_dict = {}
    while True:

        try:
            msg = listener_q.get()
        except Empty:
            LOG.debug("Empty listener queue...")
            continue

        LOG.debug("Number of threads currently alive: " +
                  str(threading.active_count()))

        LOG.info("FENGYUN3 files: " + str(fy3_files))
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

        keyname = (str(platform_name) + '_' + str(orbit_number) + '_' + str(
            start_time.strftime('%Y%m%d%H%M')))
        # Check if we have all the files before processing can start:

        status = ready2run(msg, fy3_files, jobs_dict, keyname, options)
        if status:
            # Run
            LOG.info("Ready to run...")
            LOG.debug("fy3 file = %s", fy3_files[keyname]['fy3file'])

            scene = {
                'platform_name': platform_name,
                'orbit_number': orbit_number,
                'start_time': start_time,
                'end_time': end_time,
                'sensor': sensor,
                'fy3filename': fy3_files[keyname]['fy3file']
            }

            if platform_name in FY3_SATELLITES:
                # Do processing:
                LOG.info("Level-0 to lvl1 processing on " +
                         "FY3 : Start..." + " Start time = " +
                         str(start_time))
                pool.apply_async(run_fy3_l0l1,
                                 (scene, msg, jobs_dict[keyname], publisher_q, options))
                LOG.debug("FY3 lvl1 processing sent to pool worker...")
            else:
                LOG.debug("Platform %s not supported yet...",
                          str(platform_name))

            # Block any future run on this scene for x minutes from now
            # x = 5 minutes
            thread_job_registry = threading.Timer(
                5 * 60.0,
                reset_job_registry,
                args=(jobs_dict, fy3_files, keyname))
            thread_job_registry.start()

        LOG.debug("FY3-file registry: %s", str(fy3_files))

    pool.close()
    pool.join()

    pub_thread.stop()
    listen_thread.stop()


def create_message(mda, filename, level, sensor, options):
    LOG.debug("mda: = " + str(mda))
    LOG.debug("type(mda): " + str(type(mda)))
    to_send = mda.copy()
    if isinstance(filename, (list, tuple, set)):
        del to_send['uri']
        del to_send['uid']
        to_send['dataset'] = [{
            'uri': 'file://' + fname,
            'uid': os.path.basename(fname)
        } for fname in filename]
        mtype = 'dataset'
    else:
        to_send['uri'] = ('file://' + filename)
        to_send['uid'] = os.path.basename(filename)
        mtype = 'file'
    to_send['format'] = 'CMA'
    to_send['data_processing_level'] = level
    to_send['type'] = 'HDF5'
    to_send['sensor'] = sensor

    station = options.get('station', 'unknown')
    pubish_topic = '/'.join((str(to_send['format']),
                             str(to_send['data_processing_level']),
                             station,
                             MODE,
                             'polar'
                             'direct_readout'))
    if 'publish_topic' in options:
        publish_topic = options['publish_topic']
    message = Message(publish_topic, mtype, to_send).encode()
    return message


def ready2run(message, fy3files, job_register, sceneid, options):
    """Check if we have got all the input lvl0 files and that we are
    ready to process FENGYUN3 lvl1 data.

    """

    LOG.debug("Scene identifier = " + str(sceneid))
    LOG.debug("Job register = " + str(job_register))
    if sceneid in job_register and job_register[sceneid]:
        LOG.debug("Processing of scene " + str(sceneid) +
                  " has already been launched...")
        return False

    if sceneid not in fy3files:
        fy3files[sceneid] = {}

    urlobj = urlparse(message.data['uri'])

    if 'start_time' in message.data:
        start_time = message.data['start_time']
    else:
        LOG.warning("start_time not in message!")
        start_time = None

    if (message.data['platform_name'] in FY3_SATELLITES
            and ('mersi' in message.data['sensor'] or 'mwri' in message.data['sensor'])):

        path, fname = os.path.split(urlobj.path)
        LOG.debug("path " + str(path) + " filename = " + str(fname))
        fy3files[sceneid]['fy3file'] = urlobj.path

    if 'fy3file' in fy3files[sceneid]:
        LOG.info("File ready for FENGYUN3 level-1 runner: " +
                 str(fy3files[sceneid]))

        job_register[sceneid] = datetime.utcnow()
        return True
    else:
        return False


def get_working_dir(options):
    working_dir = options.get('working_dir', '/tmp')
    if not os.path.exists(working_dir):
        try:
            os.makedirs(working_dir)
        except OSError:
            LOG.error("Failed creating working directory %s", working_dir)
            working_dir = '/tmp'
            LOG.info("Will use /tmp")

    return working_dir


def clean_utcpole_and_leapsec_files(thr_days=60):
    """Clean any old *leapsec.dat* and *utcpole.dat* backup files, older than
    *thr_days* old

    """
    from glob import glob
    from datetime import datetime, timedelta
    import os

    now = datetime.utcnow()
    deltat = timedelta(days=int(thr_days))

    # Make the list of files to clean:
    flist = glob(os.path.join(ETC_DIR, '*.dat_*'))
    for filename in flist:
        lastpart = os.path.basename(filename).split('dat_')[1]
        tobj = datetime.strptime(lastpart, "%Y%m%d%H%M")
        if (now - tobj) > deltat:
            LOG.info("File too old, cleaning: %s " % filename)
            os.remove(filename)

    return

def check_1le_files(options, thr_hours=1):
    """Check if the 1le file are available in the
    etc directory and check if they are fresh.
    Return True if fresh/new files exists, otherwise False

    """

    from glob import glob
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    tdelta = timedelta(hours=int(thr_hours))

    files_ok = True
    for bname in options['one_le']:
        LOG.info("File " + str(bname['out']) + "...")
        filename = os.path.join(ETC_DIR, bname['out'])
        if os.path.exists(filename):
            # Check how old it is:
            realpath = os.path.realpath(filename)
            # Get the timestamp in the file name:
            try:
                tstamp = os.path.basename(realpath).split('.dat_')[1]
            except IndexError:
                files_ok = False
                break
            tobj = datetime.strptime(tstamp, "%Y%m%d%H%M")

            if (now - tobj) > tdelta:
                LOG.info("File too old! File=%s " % filename)
                files_ok = False
                break
        else:
            LOG.info("No 1le file: %s" % filename)
            files_ok = False
            break

    return files_ok

def download_1le(options):
    """Download the FY3 1le file"""

    if six.PY2:
        from urllib2 import urlopen, URLError, HTTPError
    elif six.PY3:
        from urllib.request import urlopen, URLError, HTTPError
    import os
    from datetime import datetime

    LOG.info("Start downloading....")
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    for download in options['one_le']:
        try:
            usock = urlopen(download['http'])
        except HTTPError:
            LOG.warning("Failed opening file " + download['http'])
            continue

        data = usock.read()
        usock.close()
        LOG.info("Data retrieved from url...")

        # I store the files with a timestamp attached, in order not to remove
        # the existing files. In case something gets wrong in the download, we
        # can handle this by not changing the sym-links below:
        outfile = os.path.join(ETC_DIR, download['out'] + '_' + timestamp)
        linkfile = os.path.join(ETC_DIR, download['out'])
        fd = open(outfile, 'wb')
        fd.write(data)
        fd.close()

        LOG.info("Data written to file " + outfile)

        # Update the symlinks (assuming the files are okay):
        LOG.debug("Adding symlink %s -> %s", linkfile, outfile)
        if os.path.islink(linkfile):
            LOG.debug("Unlinking %s", linkfile)
            os.unlink(linkfile)

        try:
            os.symlink(outfile, linkfile)
        except OSError as err:
            LOG.warning(str(err))
    
def check_utcpole_and_leapsec_files(thr_days=14):
    """Check if the files *leapsec.dat* and *utcpole.dat* are available in the
    etc directory and check if they are fresh.
    Return True if fresh/new files exists, otherwise False

    """

    from glob import glob
    from datetime import datetime, timedelta

    now = datetime.utcnow()
    tdelta = timedelta(days=int(thr_days))

    files_ok = True
    for bname in NAVIGATION_HELPER_FILES:
        LOG.info("File " + str(bname) + "...")
        filename = os.path.join(ETC_DIR, bname)
        if os.path.exists(filename):
            # Check how old it is:
            realpath = os.path.realpath(filename)
            # Get the timestamp in the file name:
            try:
                tstamp = os.path.basename(realpath).split('.dat_')[1]
            except IndexError:
                files_ok = False
                break
            tobj = datetime.strptime(tstamp, "%Y%m%d%H%M")

            if (now - tobj) > tdelta:
                LOG.info("File too old! File=%s " % filename)
                files_ok = False
                break
        else:
            LOG.info("No navigation helper file: %s" % filename)
            files_ok = False
            break

    return files_ok


def update_utcpole_and_leapsec_files():
    """
    Function to update the ancillary data files *leapsec.dat* and
    *utcpole.dat* used in the navigation of FENGYUN3 direct readout data.

    These files need to be updated at least once every 2nd week, in order to
    achieve the best possible navigation.

    """
    if six.PY2:
        from urllib2 import urlopen, URLError, HTTPError
    elif six.PY3:
        from urllib.request import urlopen, URLError, HTTPError
    import os
    import sys
    from datetime import datetime

    # Start cleaning any possible old files:
    clean_utcpole_and_leapsec_files(DAYS_KEEP_OLD_ETC_FILES)

    try:
        usock = urlopen(URL)
    except URLError:
        LOG.warning('Failed opening url: ' + URL)
        return
    else:
        usock.close()

    LOG.info("Start downloading....")
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    for filename in NAVIGATION_HELPER_FILES:
        try:
            usock = urlopen(URL + filename)
        except HTTPError:
            LOG.warning("Failed opening file " + filename)
            continue

        data = usock.read()
        usock.close()
        LOG.info("Data retrieved from url...")

        # I store the files with a timestamp attached, in order not to remove
        # the existing files. In case something gets wrong in the download, we
        # can handle this by not changing the sym-links below:
        newname = filename + '_' + timestamp
        outfile = os.path.join(ETC_DIR, newname)
        linkfile = os.path.join(ETC_DIR, filename)
        fd = open(outfile, 'wb')
        fd.write(data)
        fd.close()

        LOG.info("Data written to file " + outfile)
        # Here we could make a check on the sanity of the downloaded files:
        # TODO!

        # Update the symlinks (assuming the files are okay):
        LOG.debug("Adding symlink %s -> %s", linkfile, outfile)
        if os.path.islink(linkfile):
            LOG.debug("Unlinking %s", linkfile)
            os.unlink(linkfile)

        try:
            os.symlink(outfile, linkfile)
        except OSError as err:
            LOG.warning(str(err))

    return


def run_fy3_l0l1(scene, message, job_id, publish_q, options):
    """Process FY3 level 0 data to level 1a/1b"""

    from subprocess import Popen, PIPE
    from glob import glob
    from trollsift import compose
    try:

        LOG.debug("Inside run_fy3_l0l1...")

        working_dir = get_working_dir(options)
        LOG.debug("Working dir = %s", str(working_dir))

        if scene['platform_name'] == 'Fengyun-3D' or scene['platform_name'] == 'fengyun 3d':
            scene['mission'] = 'FY3D'

        fy3dl0db = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/org/')
        fileout = os.path.join(fy3dl0db, compose(options['fy3_l0'], scene) + ".ORG")
        LOG.debug("fileout: %s", str(fileout))
        #MEOS CLEAR data has cadu size 1072. Rewrite to 1024
        if os.path.exists(scene['fy3filename']) and (os.path.basename(scene['fy3filename']).startswith('clear') or os.path.basename(scene['fy3filename']).startswith('rawdata')):
            LOG.info("Rewrite MEOS clear file ...")
            fd_out = open(fileout, 'wb')
            with open(scene['fy3filename'], 'rb') as fd:
                while True:
                    packet = fd.read(1072)
                    if packet:
                        fd_out.write(packet[:1024])
                    else:
                        break
            fd.close()
            fd_out.close()
            LOG.info("Complete rewrite.")
        else:
            LOG.debug("No MEOS clear file: %s", scene['fy3filename'])

        os.environ['HOME'] = options['fengyun3_home']
        # Some of the processing scripts needs the ld_library_path.
        # Can be configured or given as an env variable for the runner
        if 'ld_library_path' in options:
            os.environ['LD_LIBRARY_PATH'] = options.get('ld_library_path')
        LOG.info("Level-0 filename: " + str(fileout))
        fy3_l0_bin_dir = os.path.join(options['fengyun3_home'], "fy3dl0db", "bin")
        processing_timeout_timeout = "1h"

        for instrument in options['process_instrument_scripts_l0']:
            segfault = False
            cmdl = ["timeout", "{}".format(processing_timeout_timeout),
                    "{}/{}".format(fy3_l0_bin_dir, instrument),
                    "{}".format(os.path.basename(fileout)),
                    "{}".format(scene['mission'])
            ]

            LOG.debug("Run command: " + str(cmdl))
            fy3_unpack_proc = Popen(cmdl, shell=False, cwd=working_dir, stderr=PIPE, stdout=PIPE)

            while True:
                line = fy3_unpack_proc.stdout.readline()
                if not line:
                    break
                if 'Segmentation fault' in str(line):
                    segfault = True
                # Due to endless number of these lines they are skipped
                if 'Cadu number = ' in str(line):
                    continue
                try:
                    LOG.info("%s",line.decode('utf8').replace('\n',''))
                except UnicodeDecodeError:
                    LOG.info(line)

            while True:
                errline = fy3_unpack_proc.stderr.readline()
                if not errline:
                    break
                if 'Segmentation fault' in str(errline):
                    segfault = True
                try:
                    LOG.info("%s",errline.decode('utf8').replace('\n',''))
                except UnicodeDecodeError:
                    LOG.info(errline)

            fy3_unpack_proc.poll()
            fy3_unpack_status = fy3_unpack_proc.returncode
            LOG.debug("Return code from fy3 unpack processing = " +
                      str(fy3_unpack_status))
            if (fy3_unpack_status not in [1, None] or segfault):
                LOG.error("Failed in the FY3 unpack processing in script: %s. Skip this.", instrument)
                continue

        #Link the unpack data from l0 to l1 data directories

        gps2s = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/gps/GPS2S.DAT')
        gpsxx = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/gps/GPSXX.DAT')

        #Clean GPSXX and GPS2S if exists
        if os.path.exists(gps2s):
            os.remove(gps2s)
        if os.path.exists(gpsxx):
            os.remove(gpsxx)

        fy3dl0db_gps = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/vass_l0')

        #Link needed GPSXX ad GPS2S to actual GPSXX and GPS2S data
        gps2s_link_file = os.path.join(fy3dl0db_gps, compose(options['fy3_l0'], scene) + "_GPS2S.DAT")
        gpsxx_link_file = os.path.join(fy3dl0db_gps, compose(options['fy3_l0'], scene) + "_GPSXX.DAT")
        if os.path.exists(gps2s_link_file):
            os.link(gps2s_link_file, gps2s)
        else:
            LOG.debug("Could not find gps2s file to link to gps2s files: %s", gps2s_link_file)
        if os.path.exists(gpsxx_link_file):
            os.link(gpsxx_link_file, gpsxx)
        else:
            LOG.debug("Could not find gpsxx file to link to gpsxx files: %s", gpsxx_link_file)

        fy3_l1_home = os.path.join(options['fengyun3_home'], "fy3dl1db", "bin")

        #Paths where to find processed data
        pdp = {}
        pdp['Mersi'] = {}
        pdp['Mersi']['sensor'] = 'MERSI'
        pdp['Mersi']['fy3dl0db'] = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/mersi_l0')
        pdp['Mersi']['fy3dl1db_l0'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mersi_l0')
        pdp['Mersi']['fy3dl1db_l1'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mersi_l1')
        pdp['Mersi']['l1_files'] = ['1000M_L1B.HDF', '0250M_L1B.HDF', 'GEOQK_L1B.HDF', 'GEO1K_L1B.HDF']
        pdp['Hiras'] = {}
        pdp['Hiras']['sensor'] = 'HIRAS'
        pdp['Hiras']['fy3dl0db'] = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/hiras_l0')
        pdp['Hiras']['fy3dl1db_l0'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/hiras_l0')
        pdp['Hiras']['fy3dl1db_l1'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/hiras_l1')
        pdp['Hiras']['l1_files'] = ['L1B.HDF']
        pdp['Mwhs'] = {}
        pdp['Mwhs']['sensor'] = 'MWHSX'
        pdp['Mwhs']['fy3dl0db'] = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/vass_l0')
        pdp['Mwhs']['fy3dl1db_l0'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mwhs_l0')
        pdp['Mwhs']['fy3dl1db_l1'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mwhs_l1')
        pdp['Mwhs']['l1_files'] = ['L1B.HDF']
        pdp['Mwts'] = {}
        pdp['Mwts']['sensor'] = 'MWTSX'
        pdp['Mwts']['fy3dl0db'] = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/vass_l0')
        pdp['Mwts']['fy3dl1db_l0'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mwts_l0')
        pdp['Mwts']['fy3dl1db_l1'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mwts_l1')
        pdp['Mwts']['l1_files'] = ['L1B.HDF']
        pdp['Mwri'] = {}
        pdp['Mwri']['sensor'] = 'MWRIX'
        pdp['Mwri']['fy3dl0db'] = os.path.join(options['fengyun3_home'], 'fy3dl0db/data/vass_l0')
        pdp['Mwri']['fy3dl1db_l0'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mwri_l0')
        pdp['Mwri']['fy3dl1db_l1'] = os.path.join(options['fengyun3_home'], 'fy3dl1db/data/mwri_l1')
        pdp['Mwri']['l1_files'] = ['L1B.HDF']

        for instrument in options['process_instrument_scripts_l1']:
            segfault = False
            if 'Mersi' in instrument:
                data_conf = pdp['Mersi']
            elif 'Hiras' in instrument:
                data_conf = pdp['Hiras']
            elif 'Mwts' in instrument:
                data_conf = pdp['Mwts']
            elif 'Mwhs' in instrument:
                data_conf = pdp['Mwhs']
            elif 'Mwri' in instrument:
                data_conf = pdp['Mwri']
            else:
                LOG.warning("Unknown instrument.")
                continue

            if 'Mwri' in instrument and scene['mission'] not in MWRI_MISSIONS:
                LOG.debug("MWRI only available for %s", str(MWRI_MISSIONS))
                continue
            if 'Hiras' in instrument and scene['mission'] not in HIRAS_MISSIONS:
                LOG.debug("HIRAS only available for %s", str(HIRAS_MISSIONS))
                continue
            link_file = os.path.join(data_conf['fy3dl0db'], compose(options['fy3_l0'], scene) + "_{}.DAT".format(data_conf['sensor']))
            link_name = os.path.join(data_conf['fy3dl1db_l0'], compose(options['fy3_l0'], scene) + "_{}.DAT".format(data_conf['sensor']))
            #Clean l0 file if exists
            if os.path.exists(link_name):
                os.remove(link_name)
            #Create link from l1 data to l0 data
            if os.path.exists(link_file):
                os.link(link_file, link_name)
            else:
                LOG.debug("Could not find file from unpack to link to l1 file: %s", link_file)
            cmdl = []
            cmdl.append("timeout")
            cmdl.append("{}".format(processing_timeout_timeout))
            cmdl.append("{}/{}".format(fy3_l1_home, instrument))
            if data_conf['sensor'] != 'MWHSX':
                cmdl.append("GPSXX.DAT")
            if data_conf['sensor'] == 'MERSI':
                cmdl.append("GPS2S.DAT")
            cmdl.append("{}_{}.DAT".format(compose(options['fy3_l0'], scene),
                                   data_conf['sensor']))
            cmdl.append("{}".format(scene['mission']))
            LOG.debug("Run command: %s", str(cmdl))
            fy3lvl1b_proc = Popen(
                cmdl, shell=False, cwd=working_dir, stderr=PIPE, stdout=PIPE)

            while True:
                line = fy3lvl1b_proc.stdout.readline()
                if not line:
                    break
                if 'Segmentation fault' in str(line):
                    segfault = True
                # Due to endless number of these lines they are skipped
                if 'Cadu number = ' in str(line):
                    continue
                try:
                    LOG.info("%s",line.decode('utf8').replace('\n',''))
                except UnicodeDecodeError:
                    LOG.info(line)

            while True:
                errline = fy3lvl1b_proc.stderr.readline()
                if not errline:
                    break
                if 'Segmentation fault' in str(errline):
                    segfault = True
                try:
                    LOG.info("%s",errline.decode('utf8').replace('\n',''))
                except UnicodeDecodeError:
                    LOG.info(errline)

            fy3lvl1b_proc.poll()
            fy3lvl1b_status = fy3lvl1b_proc.returncode
            LOG.debug("Return code from fengyun3 geo-loc processing = " +
                      str(fy3lvl1b_status))
            # Apparently a return code of 1 and None is okay...
            # Verify which return codes are ok! FIXME!
            if (fy3lvl1b_status not in [0, 1, None] or segfault):
                LOG.error("Failed in the FENGYUN3 level-1 processing in script %s. Skip this.", instrument)
                continue

            l1b_files = []
            for l1_file in data_conf['l1_files']:
                fname = os.path.join(data_conf['fy3dl1db_l1'],
                                     "{}_{}_{}".format(compose(options['fy3_l0'], scene),
                                                       data_conf['sensor'],
                                                       l1_file))
                if os.path.exists(fname):
                    l1b_files.append(fname)
                else:
                    LOG.warning("Missing file: %s", fname)

            pubmsg = create_message(message.data, l1b_files, '1B', data_conf['sensor'], options)
            LOG.info("Sending: %s", pubmsg)
            publish_q.put(pubmsg)

        if isinstance(job_id, datetime):
            dt_ = datetime.utcnow() - job_id
            LOG.info("FY3 level-1b scene " + str(job_id) +
                     " finished. It took: " + str(dt_))
        else:
            LOG.warning("Job entry is not a datetime instance: " + str(job_id))

        # Start checking and downloading the luts (utcpole.dat and
        # leapsec.dat):
        LOG.info("Checking the fengyun3 utcpole and leapsec and updating " +
                 "from internet if necessary!")
        fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
        if fresh:
            LOG.info("Files in etc dir are fresh! No url downloading....")
        else:
            LOG.warning("Files in etc are non existent or too old. " +
                        "Start url fetch...")
            update_utcpole_and_leapsec_files()

        fresh_1le = check_1le_files(options)
        if fresh_1le:
            LOG.info("1le files in etc dir are fresh! No url downloading....")
        else:
            LOG.warning("1le files in etc are non existent or too old. " +
                        "Start url fetch...")
            # Need to download the 1le file
            download_1le(options)
        
    except:
        LOG.exception('Failed in run_fy3_l0l1...')
        raise

    LOG.debug("Leaving run_fy3_l0l1")


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config-file",
        required=True,
        dest="config_file",
        type=str,
        default=None,
        help="The file containing configuration parameters.")
    parser.add_argument(
        "-l",
        "--log-file",
        dest="log",
        type=str,
        default=None,
        help="The file to log to (stdout per default).")
    parser.add_argument(
        "-m",
        "--mode",
        dest="mode",
        type=str,
        default=None,
        help="MODE ie. which section to read from the config file, given at the command line.")

    args = parser.parse_args()

    print("Read config from", args.config_file)
    with open(args.config_file, 'r') as stream:
        try:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            print("Failed reading yaml config file: {} with: {}".format(filename, exc))
            raise yaml.YAMLError

    #If not MODE given on command line use from environment variable
    mode = args.mode
    if not mode:
        mode = MODE

    OPTIONS = config[mode]

    #If FENGYUN3_HOME is given in the config file, override the env variable.
    #if OPTIONS['fengyun3_home']:
    #    FENGYUN3_HOME = OPTIONS['fengyun3_home']
    ETC_DIR = os.path.join(OPTIONS['fengyun3_home'],
                           'fy3dl1db/SysData/')
        
    DAYS_BETWEEN_URL_DOWNLOAD = OPTIONS.get('days_between_url_download', 14)
    DAYS_KEEP_OLD_ETC_FILES = OPTIONS.get('days_keep_old_etc_files', 60)
    URL = OPTIONS.get('leapsec_utcpole')
    handler = logging.StreamHandler(sys.stderr)

    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        fmt=_DEFAULT_LOG_FORMAT, datefmt=_DEFAULT_TIME_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    LOG = logging.getLogger('fengyun3_runner')

    fengyun3_live_runner(OPTIONS)
