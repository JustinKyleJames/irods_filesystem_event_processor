#ifndef IRODS_BEEGFS_CHANGELOG_CONFIG_H
#define IRODS_BEEGFS_CHANGELOG_CONFIG_H

#include <map>
#include <string>

const int MAX_CONFIG_VALUE_SIZE = 256;

typedef struct irods_connection_cfg {
    std::string irods_host;
    int irods_port;
} irods_connection_cfg_t;

typedef struct beegfs_irods_connector_cfg {
    std::string beegfs_socket;
    std::string beegfs_root_path;
    std::string irods_resource_name;
    std::string irods_api_update_type;    // valid values are "direct" and "policy"
    int64_t irods_resource_id;
    unsigned int changelog_poll_interval_seconds;
    unsigned int irods_client_connect_failure_retry_seconds;
    std::string irods_client_broadcast_address;
    std::string changelog_reader_broadcast_address;
    std::string changelog_reader_push_work_address;
    std::string result_accumulator_push_address;
    unsigned int irods_updater_thread_count;
    unsigned int maximum_records_per_sql_command;
    unsigned int maximum_records_per_update_to_irods;
    unsigned int maximum_records_to_receive_from_beegfs_changelog;
    unsigned int message_receive_timeout_msec;

    // optional parameters for using storage tiering time violation
    bool set_metadata_for_storage_tiering_time_violation;
    std::string metadata_key_for_storage_tiering_time_violation;


    std::map<int, irods_connection_cfg_t> irods_connection_list;

    // map the beegfs path to irods path
    std::vector<std::pair<std::string, std::string> > register_map;

} beegfs_irods_connector_cfg_t;


int read_config_file(const std::string& filename, beegfs_irods_connector_cfg_t *config_struct);

#endif
