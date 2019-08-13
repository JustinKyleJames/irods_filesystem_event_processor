#ifndef IRODS_BEEGFS_CHANGELOG_CONFIG_H
#define IRODS_BEEGFS_CHANGELOG_CONFIG_H

#include <map>
#include <string>

const int MAX_CONFIG_VALUE_SIZE = 256;

typedef struct beegfs_event_listener_cfg {
    std::string beegfs_socket;
    std::string beegfs_root_path;
    std::string event_aggregator_address;
} beegfs_event_listener_cfg_t;


int read_config_file(const std::string& filename, beegfs_event_listener_cfg_t *config_struct);

#endif
