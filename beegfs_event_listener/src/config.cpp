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
#include "irods_filesystem_event_processor_errors.hpp"

FILE *dbgstream = stdout;
int  log_level = LOG_INFO;

int read_key_from_map(const json_map& config_map, const std::string &key, std::string& value, bool required = true) {
    auto entry = config_map.find(key);
    if (entry == config_map.end()) {
       if (required) {
           LOG(LOG_ERR, "Could not find key %s in configuration\n", key.c_str());
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


int read_config_file(const std::string& filename, beegfs_event_listener_cfg_t *config_struct) {

    if ("" == filename) {
        LOG(LOG_ERR, "read_config_file did not receive a filename\n");
        return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
    }

    if (nullptr == config_struct) {
        LOG(LOG_ERR, "Null config_struct sent to %s - %d\n", __FUNCTION__, __LINE__);
        return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

    try {
        json_map config_map{ json_file{ filename.c_str() } };

        if (0 != read_key_from_map(config_map, "beegfs_socket", config_struct->beegfs_socket)) {
            LOG(LOG_ERR, "Key beegfs_socket missing from %s\n", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        if (0 != read_key_from_map(config_map, "beegfs_root_path", config_struct->beegfs_root_path)) {
            LOG(LOG_ERR, "Key beegfs_root_path missing from %s\n", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }
        while (remove_trailing_slash(config_struct->beegfs_root_path));

        if (0 != read_key_from_map(config_map, "event_aggregator_address", config_struct->event_aggregator_address)) {
            LOG(LOG_ERR, "Key event_aggregator_address missing from %s\n", filename.c_str());
            return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
        }

        // populate config variables
        
        std::string log_level_str;

        if (0 == read_key_from_map(config_map, "log_level", log_level_str)) {
            printf("log_level_str = %s\n", log_level_str.c_str());
            set_log_level(log_level_str);
        } 

        printf("log level set to %i\n", log_level);

    } catch (std::exception& e) {
        LOG(LOG_ERR, "Could not read %s - %s\n", filename.c_str(), e.what());
        return irods_filesystem_event_processor_error::CONFIGURATION_ERROR;
    }
    return irods_filesystem_event_processor_error::SUCCESS;

}

