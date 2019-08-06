#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2012, 2014 SMHI

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>
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

"""Testing publishing from posttroll.
"""

import sys
import os
import os.path
import datetime as dt
import time

from posttroll.publisher import NoisyPublisher
from posttroll.message import Message

def send_message(topic, info, message_type):
    '''Send message with the given topic and info'''
    pub_ = NoisyPublisher("dummy_sender", 0, topic)
    pub = pub_.start()
    time.sleep(2)
    msg = Message(topic, message_type, info)
    print("Sending message: %s" % str(msg))
    pub.send(str(msg))
    pub_.stop()

def main():
    '''Main.'''

    topic = "/XLBANDANTENNA/FENGYUN3D/CLEAR"

    info_dicts = [{"uid": "clear_FY3D_8922_2019-08-05T12:26:24.764_930", "format": "MEOS", "type": "binary", "start_time": "2019-08-05T12:26:24", "orbit_number": 8922, "uri": "ssh:///data/pytroll/fengyun3/clear/clear_FY3D_8922_2019-08-05T12:26:24.764_930", "platform_name": "Fengyun-3D", "end_time": "2019-08-05T12:40:00", "sensor": ["mersi", "hiras", "vass"], "data_processing_level": "0"},]
    message_type = 'file'

    for info_dict in info_dicts:
        send_message(topic, info_dict, message_type)

if __name__ == "__main__":
    main()
