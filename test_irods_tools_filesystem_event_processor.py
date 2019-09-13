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

# TODO 
# 1) install parallel filesystem (lustre / beegfs)
# 2) create filesystem_mount_point 
# 3) mount filesystem to filesystem_mount_point 
# 4) compile connector, aggregator, and plugin
#    a) install plugin
#    b) copy connector/aggregator to /bin
#    b) copy connector/aggregator to /bin
# 5) lfs mkdir -i 3 /lustreResc/lustre01/OST0001dir
# 6) irods user needs sudo access w/o password

class Test_Filesystem_Connector(object):

    process_list = []
    zmq_begin_port = 5555
    base_register_location = '/tempZone/home/public'
    home_register_location = '/tempZone/home'
    rods_register_location = '/tempZone/home/rods'

    def __init__(self, *args, **kwargs):
        super(Test_Filesystem_Connector, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):

        os.system('echo setUpClass for %s ran' % cls.irods_api_update_type)

        # set up configuration files for connector and listener
        cls.setup_aggregator_configuration_file(cls)
        cls.setup_beegfs_listener_configuration_file(cls)

        # start connector and listeners
        # TODO - beegfs listener can't connect to socket unless root 


        #self.process_list.append(subprocess.Popen([self.listener_executable_path,  '-c', self.listener_config_file], shell=False))
        #self.listener_pid = subprocess.Popen(['sudo', self.listener_executable_path,  '-c', self.listener_config_file], shell=False).pid
        cls.process_list.append(subprocess.Popen(['/bin/filesystem_event_aggregator',  '-c', cls.aggregator_config_file], shell=False))

        # write a dummy file to mount point because beegfs doesn't read the first event after startup
        #cls.write_to_file('%s/temp' % cls.filesystem_mount_point, 'contents of temp') 

    @classmethod
    def tearDownClass(cls):

        # stop processes
        for connector in cls.process_list:
            connector.send_signal(signal.SIGINT)

    def setUp(self):

        self.clean_up_files()
        self.admin= session.make_session_for_existing_admin()
        self.hostname = lib.get_hostname()

        super(Test_Filesystem_Connector, self).setUp()

    def tearDown(self):

        super(Test_Filesystem_Connector, self).tearDown()
        self.clean_up_files()
        time.sleep(5)
        #self.admin.assert_icommand("iadmin rmresc lustreResc")

    def clean_up_files(self):

        # Beegfs is giving me the events out of order when I use rm -r or shutil.rmtree(path)
        # which messing things up so for now doing recursive removal manually.
        for path in glob.glob('%s/*' % self.filesystem_mount_point):
            self.remove_path(path)

    # recursive removal
    def remove_path(self, path):
        if os.path.isfile(path):
            try:
                os.remove(path)
            except OSError as error:
                pass
        elif os.path.isdir(path):
            for subpath in glob.glob('%s/*' % path):
                self.remove_path(subpath)
            try:
                os.rmdir(path)
            except OSError as error:
                pass

#            # don't remove the subdirectory that is assigned to MDT3
#            try:
#                if path == '/lustreResc/lustre01/MDT0001dir':
#                       pass
#                elif os.path.isfile(path):
#                    os.remove(path)
#                elif os.path.isdir(path):
#                    shutil.rmtree(path)
#            except OSError as error:
#                pass

#        for path in glob.glob("/lustreResc/lustre01/MDT0001dir/*"):
#            if os.path.isfile(path):
#                os.remove(path)
#            elif os.path.isdir(path):
#                shutil.rmtree(path)


    @staticmethod
    def write_to_file(filename, contents):

        with open(filename, 'wt') as f:
            f.write(contents)

    @staticmethod
    def append_to_file_with_newline(filename, contents):
        with open(filename, 'a') as f:
            f.write('\n')
            f.write(contents)

    @staticmethod
    def setup_aggregator_configuration_file(cls):
     
        register_map1 = {
            "physical_path": "%s/home" % cls.filesystem_mount_point,
            "irods_register_path": cls.home_register_location 
        }

        register_map2 = {
            "physical_path": "%s/rods" % cls.filesystem_mount_point,
            "irods_register_path": cls.rods_register_location 
        }

        register_map3 = {
            "physical_path": "%s" % cls.filesystem_mount_point,
            "irods_register_path": cls.base_register_location 
        }


        register_map_list = [register_map1, register_map2, register_map3]


        aggregator_config = {
            "irods_resource_name": "demoResc",
            "irods_api_update_type": cls.irods_api_update_type,
            "log_level": "LOG_ERROR",
            "irods_updater_connect_failure_retry_seconds": 30,
            "irods_updater_thread_count": 5,
            "irods_updater_sleep_time_seconds": 1,
            "irods_updater_broadcast_address": "tcp://127.0.0.1:%d" % cls.zmq_begin_port,
            "changelog_reader_broadcast_address": "tcp://127.0.0.1:%d" % (cls.zmq_begin_port + 1), 
            "event_aggregator_address": "tcp://127.0.0.1:%d" % (cls.zmq_begin_port + 2),
            "maximum_records_per_update_to_irods": 200,
            "maximum_records_per_sql_command": 1,
            "maximum_queued_records": 100000,
            "message_receive_timeout_msec": 2000,
            "set_metadata_for_storage_tiering_time_violation": "true",
            "metadata_key_for_storage_tiering_time_violation": "irods::access_time",
            "register_map": register_map_list

        }

        with open(cls.aggregator_config_file, 'wt') as f:
            json.dump(aggregator_config, f, indent=4, ensure_ascii=False)

    @staticmethod
    def setup_beegfs_listener_configuration_file(cls):

        beegfs_listener_config = {
            "beegfs_socket": "/tmp/beegfslog",
            "beegfs_root_path": cls.filesystem_mount_point,
            "event_aggregator_address": "tcp://127.0.0.1:%d" % (cls.zmq_begin_port + 2),
            "log_level": "LOG_ERROR"
        }

        with open(cls.listener_config_file, 'wt') as f:
            json.dump(beegfs_listener_config, f, indent=4, ensure_ascii=False)

    def test_write_to_file_mountpoint(self):

        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
        time.sleep(5)
        self.admin.assert_icommand(['ils', self.base_register_location], 'STDOUT_MULTILINE', ['file1'])
        self.admin.assert_icommand(['iget', '%s/file1' % self.base_register_location, '-'], 'STDOUT_SINGLELINE', 'contents of file1')

#    def test_write_to_file_mountpoint(self):
#
#        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
#        time.sleep(3)
#        self.admin.assert_icommand(['ils', '%s/file1' % self.base_register_location], 'STDOUT_MULTILINE', ['  %s/file1' % self.base_register_location])
#        self.admin.assert_icommand(['iget', '%s/file1' % self.base_register_location, '-'], 'STDOUT_MULTILINE', ['contents of file1'])
#
#    def test_write_to_file_mountpoint(self):
#
#        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
#        time.sleep(3)
#        self.admin.assert_icommand(['ils', '%s/file1' % self.base_register_location], 'STDOUT_MULTILINE', ['  %s/file1' % self.base_register_location])
#        self.admin.assert_icommand(['iget', '%s/file1' % self.base_register_location, '-'], 'STDOUT_MULTILINE', ['contents of file1'])

    def test_append_to_file(self):

        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
        self.append_to_file_with_newline('%s/file1' % self.filesystem_mount_point, 'line2')
        time.sleep(5)
        self.admin.assert_icommand(['iget', '%s/file1' % self.base_register_location, '-'], 'STDOUT_MULTILINE', ['contents of file1', 'line2'])

    def test_move_file(self):
        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
        time.sleep(3)
        lib.execute_command(['mv',  '%s/file1' % self.filesystem_mount_point, '%s/file2' % self.filesystem_mount_point])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '%s/file2' % self.base_register_location], 'STDOUT_MULTILINE', ['  %s/file2' % self.base_register_location])

    def test_make_directory(self):
        lib.execute_command(['mkdir',  '%s/dir1' % self.filesystem_mount_point])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '%s/dir1' % self.base_register_location], 'STDOUT_MULTILINE', ['%s/dir1:' % self.base_register_location])

    def test_move_file_to_directory(self):
        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
        lib.execute_command(['mkdir',  '%s/dir1' % self.filesystem_mount_point])
        lib.execute_command(['mv',  '%s/file1' % self.filesystem_mount_point, '%s/dir1' % self.filesystem_mount_point])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '%s/dir1' % self.base_register_location], 'STDOUT_MULTILINE', ['%s/dir1:' % self.base_register_location, '  file1'])

    def test_move_directory_with_file_to_directory(self):
        lib.execute_command(['mkdir',  '%s/dir1' % self.filesystem_mount_point])
        lib.execute_command(['mkdir',  '%s/dir2' % self.filesystem_mount_point])
        self.write_to_file('%s/dir1/file1' % self.filesystem_mount_point, 'contents of file1') 
        time.sleep(3)
        lib.execute_command(['mv',  '%s/dir1' % self.filesystem_mount_point, '%s/dir2' % self.filesystem_mount_point])
        time.sleep(3)
        self.admin.assert_icommand(['ils', '%s/dir2/dir1' % self.base_register_location], 'STDOUT_MULTILINE', ['%s/dir2/dir1:' % self.base_register_location, '  file1'])
        self.admin.assert_icommand(['ils', '-L', '%s/dir2/dir1/file1' % self.base_register_location], 'STDOUT_SINGLELINE', 'generic    %s/dir2/dir1/file1' % self.filesystem_mount_point)
        self.admin.assert_icommand(['iget', '%s/dir2/dir1/file1' % self.base_register_location, '-'], 'STDOUT_SINGLELINE', 'contents of file1')

# commenting out until I can get this to work in BeeGFS
#    def test_recursive_remove(self):
#        lib.execute_command(['mkdir',  '%s/dir1' % self.filesystem_mount_point])
#        self.write_to_file('%s/file1' % self.filesystem_mount_point, 'contents of file1') 
#        time.sleep(3)
#        lib.execute_command(['rm',  '-rf', '%s/dir1' % self.filesystem_mount_point])
#        time.sleep(3)
#        self.admin.assert_icommand(['ils', '%s/dir1' % self.base_register_location], 'STDERR_SINGLELINE', 'does not exist')

#    def perform_multi_mdt_tests(self):
#
#        self.write_to_file('%s/file1', 'contents of file1') 
#        time.sleep(3)
#        self.admin.assert_icommand(['ils', '%s/file1'], 'STDOUT_MULTILINE', ['  %s/file1'])
#        self.admin.assert_icommand(['iget', '%s/file1', '-'], 'STDOUT_MULTILINE', ['contents of file1'])
#
#        lib.execute_command(['mv',  '%s/file1', '/lustreResc/lustre01/MDT0001dir/file1'])
#        time.sleep(3)
#        self.admin.assert_icommand(['ils', '%s/file1'], 'STDERR_SINGLELINE', 'does not exist')
#        self.admin.assert_icommand(['ils', '%s/MDT0001dir/file1'], 'STDOUT_MULTILINE', ['  %s/MDT0001dir/file1'])
#
#    def test_lustre_direct(self):
#        config_file = '/etc/irods/MDT0000.json'
#        self.setup_configuration_file(config_file, 'direct', 'lustre01-MDT0000', 5555)
#        self.process_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file], shell=False))
#        time.sleep(10)
#        self.perform_standard_tests()
#
#    def test_lustre_policy(self):
#        config_file = '/etc/irods/MDT0000.json'
#        self.setup_configuration_file(config_file, 'policy', 'lustre01-MDT0000', 5555)
#        self.process_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file], shell=False))
#        time.sleep(10)
#        self.perform_standard_tests()
#        #self.connector_process.send_signal(signal.SIGINT) 
#
#    def test_lustre_multi_mdt(self):
#        config_file1 = '/etc/irods/MDT0000.json'
#        self.setup_configuration_file(config_file1, 'direct', 'lustre01-MDT0000', 5555)
#        self.connector_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file1], shell=False))
#        config_file2 = '/etc/irods/MDT0001.json'
#        self.setup_configuration_file(config_file2, 'direct', 'lustre01-MDT0001', 5565)
#        self.connector_list.append(subprocess.Popen(['/bin/lustre_irods_connector',  '-c', config_file2], shell=False))
#        time.sleep(10)
#        self.perform_multi_mdt_tests()

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
        super(Test_Filesystem_Connector_Beegfs_Policy, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(Test_Filesystem_Connector_Beegfs_Policy, cls).tearDownClass()

