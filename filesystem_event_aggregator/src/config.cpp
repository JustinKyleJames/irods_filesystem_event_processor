#include <jeayeson/jeayeson.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>
#include <iostream>
#include <sstream>
#include <typeinfo>
#include <string>
#include <algorithm>

#include "config.hpp"
#include "logging.hpp"
#include "../../common/irods_filesystem_event_processor_errors.hpp"

FILE *dbgstream = stdout;
int  log_level = LOG_INFO;

thread_local char *thread_identifier;

int read_key_from_map(const json_map& config_map, const std::string &key, std::string& value, bool required = true) {
    auto entry = config_map.find(key);
    if (entry == config_map.end()) {
       if (required) {
           LOG(LOG_ERR, "Could not find key %s in configuration", key.c_str());
           return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
       } else {
           // return error here just indicates the caller should
           // set a default value
           return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
       }
    }
    std::stringstream tmp;
    tmp << entry->second;
    value = tmp.str();

    // remove quotes
    value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
    return irods_filesystem_event_processor_error::SUCCESS;
}

void set_log_level(const std::string& log_level_str) {
    if ("LOG_FATAL" == log_level_str) {
        log_level = LOG_FATAL; 
    } else if ("LOG_ERR" == log_level_str || "LOG_ERROR" == log_level_str) {
        log_level = LOG_ERR;
    } else if ("LOG_WARN" == log_level_str) {
        log_level = LOG_WARN;
    } else if ("LOG_INFO" == log_level_str) {
        log_level = LOG_INFO;
    } else if ("LOG_DBG" == log_level_str || "LOG_DEBUG" == log_level_str)  {
        log_level = LOG_DBG;
    }
}   

bool remove_trailing_slash(std::string& path) {

    if (path.length() > 0) {
        std::string::iterator it = path.end() - 1;
        if (*it == '/')
        {
            path.erase(it);
            return true;
        }
    }

    return false;
}


int read_config_file(const std::string& filename, filesystem_event_aggregator_cfg_t *config_struct) {

    if ("" == filename) {
        LOG(LOG_ERR, "read_config_file did not receive a filename");
        return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
    }

    if (nullptr == config_struct) {
        LOG(LOG_ERR, "Null config_struct sent to %s - %d", __FUNCTION__, __LINE__);
        return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

    std::string log_level_str;
    std::string irods_updater_connect_failure_retry_seconds_str;
    std::string irods_updater_sleep_time_seconds_str;
    std::string irods_updater_thread_count_str;
    std::string maximum_records_per_update_to_irods_str;
    std::string maximum_records_per_sql_command_str;
    std::string maximum_queued_records_str;
    std::string message_receive_timeout_msec_str;
    std::string time_violation_setting_str;

    try {
        json_map config_map{ json_file{ filename.c_str() } };

        if (0 != read_key_from_map(config_map, "irods_resource_name", config_struct->irods_resource_name)) {
            LOG(LOG_ERR, "Key resource_name missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_api_update_type", config_struct->irods_api_update_type)) {
            std::transform(config_struct->irods_api_update_type.begin(), config_struct->irods_api_update_type.end(), 
                    config_struct->irods_api_update_type.begin(), ::tolower);
            LOG(LOG_ERR, "Key irods_api_update_type missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_updater_connect_failure_retry_seconds", irods_updater_connect_failure_retry_seconds_str)) {
            LOG(LOG_ERR, "Key irods_updater_connect_failure_retry_seconds missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_updater_broadcast_address", config_struct->irods_updater_broadcast_address)) {
            LOG(LOG_ERR, "Key irods_updater_broadcast_address missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "changelog_reader_broadcast_address", config_struct->changelog_reader_broadcast_address)) {
            LOG(LOG_ERR, "Key changelog_reader_broadcast_address missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "event_aggregator_address", config_struct->event_aggregator_address)) {
            LOG(LOG_ERR, "Key event_aggregator_address missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        if (0 != read_key_from_map(config_map, "irods_updater_thread_count", irods_updater_thread_count_str)) {
            LOG(LOG_ERR, "Key irods_updater_thread_count missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "irods_updater_sleep_time_seconds", irods_updater_sleep_time_seconds_str)) {
            LOG(LOG_ERR, "Key irods_updater_sleep_time_seconds missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "maximum_queued_records", maximum_queued_records_str)) {
            LOG(LOG_ERR, "Key maximum_queued_records missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "maximum_records_per_sql_command", maximum_records_per_sql_command_str)) {
            LOG(LOG_ERR, "Key maximum_records_per_sql_command missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "maximum_records_per_update_to_irods", maximum_records_per_update_to_irods_str)) {
            LOG(LOG_ERR, "Key maximum_records_per_upate_to_irods missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        if (0 != read_key_from_map(config_map, "message_receive_timeout_msec", message_receive_timeout_msec_str)) {
            LOG(LOG_ERR, "Key message_receive_timeout_msec missing from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        if (0 != read_key_from_map(config_map, "set_metadata_for_storage_tiering_time_violation", time_violation_setting_str, false)) {
            config_struct->set_metadata_for_storage_tiering_time_violation = false;
        } else {
            std::transform(time_violation_setting_str.begin(), time_violation_setting_str.end(), time_violation_setting_str.begin(), ::tolower);
            if (time_violation_setting_str == "true") {
                config_struct->set_metadata_for_storage_tiering_time_violation = true;
            } else {
                config_struct->set_metadata_for_storage_tiering_time_violation = false;
            }
        }


        if (0 != read_key_from_map(config_map, "metadata_key_for_storage_tiering_time_violation", config_struct->metadata_key_for_storage_tiering_time_violation, false)) {
            config_struct->metadata_key_for_storage_tiering_time_violation = "irods::access_time";
        } 

        // read register_map
        try {
            auto &register_map_array(config_map.get<json_array>("register_map"));

            for (auto& iter : register_map_array) {
                auto path_map_entry = iter.as<json_map>();

                std::string physical_path, irods_register_path;

                if (0 != read_key_from_map(path_map_entry, "physical_path", physical_path)) {
                    LOG(LOG_ERR, "Key physical_path missing from entry in register_map of json file %s", filename.c_str());
                    return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
                }
                if (0 != read_key_from_map(path_map_entry, "irods_register_path", irods_register_path)) {
                    LOG(LOG_ERR, "Key irods_register_path missing from entry in register_map of json file %s", filename.c_str());
                    return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
                }

                // remove trailing slashes
                while (remove_trailing_slash(physical_path));
                while (remove_trailing_slash(irods_register_path));

                std::pair<std::string, std::string> path_entry_pair;
                path_entry_pair = std::make_pair(physical_path, irods_register_path);
                config_struct->register_map.push_back(path_entry_pair);
            }

        } catch (const std::exception& e) {
            LOG(LOG_ERR, "Could not read register_map array from %s", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        // populate config variables

        if (0 == read_key_from_map(config_map, "log_level", log_level_str)) {
            printf("log_level_str = %s\n", log_level_str.c_str());
            set_log_level(log_level_str);
        } 

        printf("log level set to %i\n", log_level);

        // convert irods_api_update_type to lowercase 
        std::transform(config_struct->irods_api_update_type.begin(), config_struct->irods_api_update_type.end(), 
                 config_struct->irods_api_update_type.begin(), ::tolower);

        // error if the setting is not "direct" or "policy"
        if (config_struct->irods_api_update_type != "direct" && config_struct->irods_api_update_type != "policy") { 
            LOG(LOG_ERR, "Could not parse irods_api_update_type.  It must be either \"direct\" or \"policy\".");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        try {
            config_struct->irods_updater_connect_failure_retry_seconds = boost::lexical_cast<unsigned int>(irods_updater_connect_failure_retry_seconds_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_updater_connect_failure_retry_seconds as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }


        try {
            config_struct->irods_updater_thread_count = boost::lexical_cast<unsigned int>(irods_updater_thread_count_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_updater_thread_count as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        try {
            config_struct->irods_updater_sleep_time_seconds = boost::lexical_cast<unsigned int>(irods_updater_sleep_time_seconds_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse irods_updater_sleep_time_seconds as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        try {
            config_struct->maximum_records_per_update_to_irods = boost::lexical_cast<unsigned int>(maximum_records_per_update_to_irods_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse maximum_records_per_update_to_irods as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        try {
            config_struct->maximum_records_per_sql_command = boost::lexical_cast<unsigned int>(maximum_records_per_sql_command_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse maximum_records_per_sql_command as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        try {
            config_struct->maximum_queued_records = boost::lexical_cast<unsigned int>(maximum_queued_records_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse maximum_queued_records as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }


        try {
            config_struct->message_receive_timeout_msec = boost::lexical_cast<unsigned int>(message_receive_timeout_msec_str);
        } catch (boost::bad_lexical_cast& e) {
            LOG(LOG_ERR, "Could not parse message_receive_timeout_msec as an integer.");
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }


        // read individual thread connection parameters
        boost::format format_object("thread_%i_connection_parameters");
        for (unsigned int i = 0; i < config_struct->irods_updater_thread_count; ++i) {


            std::string key = str(format_object % i); 

            auto entry = config_map.find(key);
            if (config_map.end() != entry) {
                irods_connection_cfg_t config_entry;
                auto tmp = entry->second["irods_host"];
                std::stringstream ss;
                ss << tmp;
                std::string value = ss.str();
                value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
                if ("null" == value) {
                    LOG(LOG_ERR, "Could not read irods_host for connection %u.  Either define it or leave off connection 1 paramters to use defaults from the iRODS environment.", i);
                    return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
                }

                config_entry.irods_host = value;

                // clear stringstream and read irods_port
                ss.str( std::string() );
                ss.clear();
                tmp = entry->second["irods_port"];
                ss << tmp;
                value = ss.str();
                value.erase(remove(value.begin(), value.end(), '\"' ), value.end());
                try {
                    config_entry.irods_port = boost::lexical_cast<unsigned int>(value);
                } catch (boost::bad_lexical_cast& e) {
                    LOG(LOG_ERR, "Could not parse port %s as an integer.", value.c_str());
                    return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
                }
                config_struct->irods_connection_list[i] = config_entry;

            }
        } 

    } catch (std::exception& e) {
        LOG(LOG_ERR, "Could not read %s - %s", filename.c_str(), e.what());
        return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
    }
    return irods_filesystem_event_processor_error::SUCCESS;

}

