#ifndef IRODS_FILE_SYSTEM_EVENT_AGGREGATOR_CONFIG_H
#define IRODS_FILE_SYSTEM_EVENT_AGGREGATOR_CONFIG_H

#include <map>
#include <string>

const int MAX_CONFIG_VALUE_SIZE = 256;

typedef struct irods_connection_cfg {
    std::string irods_host;
    int irods_port;
} irods_connection_cfg_t;

typedef struct filesystem_event_aggregator_cfg {
    std::string irods_resource_name;
    std::string irods_api_update_type;    // valid values are "direct" and "policy"
    int64_t irods_resource_id;
    unsigned int irods_client_connect_failure_retry_seconds;
    std::string irods_client_broadcast_address;
    std::string changelog_reader_broadcast_address;
    std::string result_accumulator_push_address;
    std::string event_aggregator_address;   // must be the same as the event listener's event_aggregator_address
    unsigned int irods_updater_thread_count;
    unsigned int maximum_records_per_sql_command;
    unsigned int maximum_records_per_update_to_irods;
    unsigned int maximum_queued_records;
    unsigned int message_receive_timeout_msec;

    // optional parameters for using storage tiering time violation
    bool set_metadata_for_storage_tiering_time_violation;
    std::string metadata_key_for_storage_tiering_time_violation;

    std::map<int, irods_connection_cfg_t> irods_connection_list;

    // map the physical path to irods path
    std::vector<std::pair<std::string, std::string> > register_map;

} filesystem_event_aggregator_cfg_t;


int read_config_file(const std::string& filename, filesystem_event_aggregator_cfg_t *config_struct);

#endif
