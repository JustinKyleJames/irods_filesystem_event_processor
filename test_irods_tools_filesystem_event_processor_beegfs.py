#import itertools
import re
import sys
import json
import subprocess
import time
import signal
import glob
import shutil
import os

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from .resource_suite import ResourceBase

from .. import test
from . import settings
from . import session
from .. import lib
from ..configuration import IrodsConfig

from test_irods_tools_filesystem_event_processor import Test_Filesystem_Connector

class Test_Filesystem_Connector_Beegfs(Test_Filesystem_Connector):

    filesystem_mount_point = '/mnt/beegfs'
    aggregator_config_file = '/etc/irods/aggregator_config.json'
    listener_config_file = '/etc/irods/beegfs_listener_config.json'
    listener_executable_path = '/bin/beegfs_event_listener'
    resource_name = 'demoResc'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Beegfs, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.setup_listener_configuration_file()
        super(Test_Filesystem_Connector_Beegfs, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Beegfs, cls).tearDownClass()


    @classmethod
    def setup_listener_configuration_file(cls):

        listener_config = {
            "beegfs_socket": "/tmp/beegfslog",
            "beegfs_root_path": cls.filesystem_mount_point,
            "event_aggregator_address": "tcp://127.0.0.1:%d" % (cls.zmq_begin_port + 2),
            "log_level": "LOG_ERROR"
        }

        with open(cls.listener_config_file, 'wt') as f:
            json.dump(listener_config, f, indent=4, ensure_ascii=False)

class Test_Filesystem_Connector_Beegfs_Direct(Test_Filesystem_Connector_Beegfs, unittest.TestCase):
   
    irods_api_update_type = 'direct'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Beegfs_Direct, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Direct, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Direct, cls).tearDownClass()


class Test_Filesystem_Connector_Beegfs_Policy(Test_Filesystem_Connector_Beegfs, unittest.TestCase):
    
    irods_api_update_type = 'policy'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Beegfs_Policy, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Policy, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Policy, cls).tearDownClass()

