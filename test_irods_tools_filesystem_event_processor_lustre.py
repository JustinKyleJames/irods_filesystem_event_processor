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

class Test_Filesystem_Connector_Lustre(Test_Filesystem_Connector):
    
    filesystem_mount_point = '/cfs/test/scratch'
    aggregator_config_file = '/etc/irods/aggregator_config.json'
    listener_config_file = '/etc/irods/lustre_listener_config.json'
    listener_executable_path = '/root/irods_filesystem_event_processor/lustre_event_listener/bld/lustre_event_listener'
    resource_name = 'lustre-test'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Lustre, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.setup_listener_configuration_file()
        super(Test_Filesystem_Connector_Lustre, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Lustre, cls).tearDownClass()


    @classmethod
    def setup_listener_configuration_file(cls):

        listener_config = {
            "mdtname": "test-MDT0000",
            "changelog_reader": "cl3",
            "lustre_root_path": cls.filesystem_mount_point,
            "log_level": "LOG_ERROR",
            "event_aggregator_address": "tcp://127.0.0.1:%d" % (cls.zmq_begin_port + 2),
            "sleep_time_when_changelog_empty_seconds": 1
        }

        with open(cls.listener_config_file, 'wt') as f:
            json.dump(listener_config, f, indent=4, ensure_ascii=False)

class Test_Filesystem_Connector_Lustre_Direct(Test_Filesystem_Connector_Lustre, unittest.TestCase):
   
    irods_api_update_type = 'direct'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Lustre_Direct, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        super(Test_Filesystem_Connector_Lustre_Direct, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Lustre_Direct, cls).tearDownClass()


class Test_Filesystem_Connector_Lustre_Policy(Test_Filesystem_Connector_Lustre , unittest.TestCase):
    
    irods_api_update_type = 'policy'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Lustre_Policy, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        super(Test_Filesystem_Connector_Lustre_Policy, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Lustre_Policy, cls).tearDownClass()

