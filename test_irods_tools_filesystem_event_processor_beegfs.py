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


class Test_Filesystem_Connector_Beegfs_Direct(Test_Filesystem_Connector, unittest.TestCase):
   
    filesystem_mount_point = '/mnt/beegfs'
    aggregator_config_file = '/etc/irods/aggregator_config.json'
    listener_config_file = '/etc/irods/beegfs_listener_config.json'
    listener_executable_path = '/bin/beegfs_event_listener'
    irods_api_update_type = 'direct'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Beegfs_Direct, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.setup_beegfs_listener_configuration_file(cls)
        super(Test_Filesystem_Connector_Beegfs_Direct, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Direct, cls).tearDownClass()


class Test_Filesystem_Connector_Beegfs_Policy(Test_Filesystem_Connector , unittest.TestCase):
    
    filesystem_mount_point = '/mnt/beegfs'
    aggregator_config_file = '/etc/irods/aggregator_config.json'
    listener_config_file = '/etc/irods/beegfs_listener_config.json'
    listener_executable_path = '/bin/beegfs_event_listener'
    irods_api_update_type = 'policy'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector_Beegfs_Policy, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        cls.setup_beegfs_listener_configuration_file(cls)
        super(Test_Filesystem_Connector_Beegfs_Policy, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Policy, cls).tearDownClass()

