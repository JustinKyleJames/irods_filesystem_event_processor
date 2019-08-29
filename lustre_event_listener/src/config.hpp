#ifndef IRODS_LUSTRE_CHANGELOG_CONFIG_H
#define IRODS_LUSTRE_CHANGELOG_CONFIG_H

#include <map>
#include <string>

const int MAX_CONFIG_VALUE_SIZE = 256;

typedef struct lustre_event_listener_cfg {
    std::string mdtname;
    std::string changelog_reader;
    std::string lustre_root_path;
    std::string event_aggregator_address;
    unsigned int sleep_time_when_changelog_empty_seconds;
} lustre_event_listener_cfg_t;


int read_config_file(const std::string& filename, lustre_event_listener_cfg_t *config_struct);

#endif
