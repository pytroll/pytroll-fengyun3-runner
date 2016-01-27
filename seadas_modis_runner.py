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

import ConfigParser
import logging
LOG = logging.getLogger(__name__)

import os
import sys
import shutil
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


def reset_job_registry(objdict, key):
    """Remove job key from registry"""
    LOG.debug("Release/reset job-key " + str(key) + " from job registry")
    if key in objdict:
        objdict.pop(key)
    else:
        LOG.warning("Nothing to reset/release - " +
                    "Register didn't contain any entry matching: " +
                    str(key))
    return


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

    # Start checking and dowloading the luts (utcpole.dat and
    # leapsec.dat):
    LOG.info("Checking the modis luts and updating " +
             "from internet if necessary!")
    fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
    if fresh:
        LOG.info(
            "Files in etc dir are fresh! No url downloading....")
    else:
        LOG.warning("Files in etc are non existent or too old. " +
                    "Start url fetch...")
        update_utcpole_and_leapsec_files()

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
            else:
                LOG.debug(
                    "Platform %s not supported yet...", str(platform_name))

            # Block any future run on this scene for x minutes from now
            # x = 5 minutes
            thread_job_registry = threading.Timer(
                5 * 60.0, reset_job_registry, args=(jobs_dict, keyname))
            thread_job_registry.start()

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
            LOG.info("Level-0 to lvl1 processing on Terra: Start..." +
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

    if len(eosfiles[sceneid]) > 0:
        LOG.info("Files ready for MODIS level-1 runner: " +
                 str(eosfiles[sceneid]))

        job_register[sceneid] = datetime.utcnow()
        return True
    else:
        return False


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
            LOG.info("File to old, cleaning: %s " % filename)
            os.remove(filename)

    return


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
    *utcpole.dat* used in the navigation of MODIS direct readout data.

    These files need to be updated at least once every 2nd week, in order to
    achieve the best possible navigation.

    """
    import urllib2
    import os
    import sys
    from datetime import datetime

    # Start cleaning any possible old files:
    clean_utcpole_and_leapsec_files(DAYS_KEEP_OLD_ETC_FILES)

    try:
        usock = urllib2.urlopen(URL)
    except urllib2.URLError:
        LOG.warning('Failed opening url: ' + URL)
        return
    else:
        usock.close()

    LOG.info("Start downloading....")
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M')
    for filename in NAVIGATION_HELPER_FILES:
        try:
            usock = urllib2.urlopen(URL + filename)
        except urllib2.HTTPError:
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
        fd = open(outfile, 'w')
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


def run_terra_l0l1(scene, job_id, publish_q):
    """Process Terra MODIS level 0 PDS data to level 1a/1b"""

    from subprocess import Popen, PIPE, STDOUT
    from glob import glob

    try:

        LOG.debug("Inside run_terra_l0l1...")

        working_dir = get_working_dir()

        #fdwork = os.open(working_dir, os.O_RDONLY)
        # os.fchdir(fdwork)

        LOG.debug("Working dir = %s", str(working_dir))

        level1b_home = OPTIONS['level1b_home']
        LOG.debug("level1b_home = %s", level1b_home)
        filetype_terra = OPTIONS['filetype_terra']
        LOG.debug("filetype_terra = %s", OPTIONS['filetype_terra'])
        geofile_terra = OPTIONS['geofile_terra']
        level1a_terra = OPTIONS['level1a_terra']
        level1b_terra = OPTIONS['level1b_terra']
        level1b_250m_terra = OPTIONS['level1b_250m_terra']
        level1b_500m_terra = OPTIONS['level1b_500m_terra']

        # Get the observation time from the filename as a datetime object:
        LOG.debug("pdsfilename = %s", scene['pdsfilename'])
        bname = os.path.basename(scene['pdsfilename'])
        obstime = datetime.strptime(bname, filetype_terra)
        LOG.debug("bname = %s obstime = %s", str(bname), str(obstime))

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

        LOG.debug("Do a file globbing to check for existing level-1b files:")
        mod01files = glob("%s/%s*hdf" % (level1b_home, firstpart))
        if len(mod01files) > 0:
            LOG.warning(
                "Level 1 file for this scene already exists: %s", mod01files[0])

        LOG.info("Level-1 filename: " + str(mod01_file))
        modisl1_home = os.path.join(SEADAS_HOME, "ocssw/run/scripts")
        cmdl = ["%s/modis_L1A.py" % modisl1_home,
                "--verbose",
                "--mission=T",
                "--startnudge=15",
                "--stopnudge=15",
                "-o%s" % (os.path.basename(mod01_file)),
                scene['pdsfilename']]

        LOG.debug("Run command: " + str(cmdl))
        modislvl1b_proc = Popen(cmdl, shell=False,
                                cwd=working_dir,
                                stderr=PIPE, stdout=PIPE)

        while True:
            line = modislvl1b_proc.stdout.readline()
            if not line:
                break
            LOG.info(line)

        while True:
            errline = modislvl1b_proc.stderr.readline()
            if not errline:
                break
            LOG.info(errline)

        modislvl1b_proc.poll()
        modislvl1b_status = modislvl1b_proc.returncode
        LOG.debug(
            "Return code from modis lvl-1a processing = " + str(modislvl1b_status))
        if modislvl1b_status != 0:
            LOG.error("Failed in the Terra level-1 processing!")
            return None

        shutil.move(os.path.join(working_dir,
                                 os.path.basename(mod01_file)),
                    mod01_file)

        # Next run the geolocation and the level-1b file:

        # modis_GEO.py --verbose --enable-dem --entrained --disable-download
        # $level1a_file
        cmdl = ["%s/modis_GEO.py" % modisl1_home,
                "--verbose",
                "--enable-dem", "--entrained", "--disable-download",
                "-o%s" % (os.path.basename(mod03_file)),
                mod01_file]

        LOG.debug("Run command: " + str(cmdl))
        modislvl1b_proc = Popen(cmdl, shell=False,
                                cwd=working_dir,
                                stderr=PIPE, stdout=PIPE)

        while True:
            line = modislvl1b_proc.stdout.readline()
            if not line:
                break
            LOG.info(line)

        while True:
            errline = modislvl1b_proc.stderr.readline()
            if not errline:
                break
            LOG.info(errline)

        modislvl1b_proc.poll()
        modislvl1b_status = modislvl1b_proc.returncode
        LOG.debug(
            "Return code from modis geo-loc processing = " + str(modislvl1b_status))
        if modislvl1b_status != 0:
            LOG.error("Failed in the Terra level-1 processing!")
            return None

        shutil.move(os.path.join(working_dir,
                                 os.path.basename(mod03_file)),
                    mod03_file)

        # modis_L1B.py --verbose $level1a_file $geo_file
        cmdl = ["%s/modis_L1B.py" % modisl1_home,
                "--verbose",
                "-okm %s" % os.path.basename(mod021km_file),
                "-hkm %s" % os.path.basename(mod02hkm_file),
                "-qkm %s" % os.path.basename(mod02qkm_file),
                mod01_file, mod03_file]

        LOG.debug("Run command: " + str(cmdl))
        modislvl1b_proc = Popen(cmdl, shell=False,
                                cwd=working_dir,
                                stderr=PIPE, stdout=PIPE)

        while True:
            line = modislvl1b_proc.stdout.readline()
            if not line:
                break
            LOG.info(line)

        while True:
            errline = modislvl1b_proc.stderr.readline()
            if not errline:
                break
            LOG.info(errline)

        modislvl1b_proc.poll()
        modislvl1b_status = modislvl1b_proc.returncode
        LOG.debug(
            "Return code from modis lvl1b processing = " + str(modislvl1b_status))
        if modislvl1b_status != 0:
            LOG.error("Failed in the Terra level-1 processing!")
            return None

        for fname in [mod021km_file, mod02hkm_file, mod02qkm_file]:
            shutil.move(os.path.join(working_dir,
                                     os.path.basename(fname)),
                        fname)

        # Start checking and dowloading the luts (utcpole.dat and
        # leapsec.dat):
        LOG.info("Checking the modis luts and updating " +
                 "from internet if necessary!")
        fresh = check_utcpole_and_leapsec_files(DAYS_BETWEEN_URL_DOWNLOAD)
        if fresh:
            LOG.info(
                "Files in etc dir are fresh! No url downloading....")
        else:
            LOG.warning("Files in etc are non existent or too old. " +
                        "Start url fetch...")
            update_utcpole_and_leapsec_files()

    except:
        LOG.exception('Failed in run_terra_l0l1...')
        raise

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
