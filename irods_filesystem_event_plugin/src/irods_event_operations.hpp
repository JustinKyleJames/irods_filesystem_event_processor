// =-=-=-=-=-=-=-
// irods includes
#include "apiHandler.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_api_call.hpp"
#include "irods_re_serialization.hpp"
#include "objStat.h"
#include "icatHighLevelRoutines.hpp"
#include "irods_virtual_path.hpp"
#include "miscServerFunct.hpp"
#include "irods_configuration_keywords.hpp"
#include "rodsType.h"

#include "boost/lexical_cast.hpp"
#include "boost/filesystem.hpp"

#include "database_routines.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <vector>

// json header
//#include <jeayeson/jeayeson.hpp>

// capn proto
#pragma push_macro("LIST")
#undef LIST

#pragma push_macro("ERROR")
#undef ERROR

#include "../../common/change_table_avro.hpp"

#pragma pop_macro("LIST")
#pragma pop_macro("ERROR")

#ifndef IRODS_FILESYSTEM_EVENT_OPERATIONS_H
#define IRODS_FILESYSTEM_EVENT_OPERATIONS_H

void handle_create(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_batch_create(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id,
        const std::string& resource_name, const std::vector<std::string>& object_identifier_list, const std::vector<std::string>& physical_path_list,
        const std::vector<std::string>& object_name_list, const std::vector<std::string>& target_parent_object_identifier_list,
        const std::vector<int64_t>& file_size_list, const int64_t& maximum_records_per_sql_command, rsComm_t* _comm, icatSessionStruct *icss, const rodsLong_t& user_id,
        bool set_metadata_for_storage_tiering_time_violation, const std::string& metadata_key_for_storage_tiering_time_violation);

void handle_mkdir(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_other(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_rename_file(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_rename_dir(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_unlink(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_batch_unlink(const std::vector<std::string>& object_identifier_list, const int64_t& resource_id, 
        const int64_t& maximum_records_per_sql_command, rsComm_t* _comm, icatSessionStruct *icss); 

void handle_rmdir(const std::vector<std::pair<std::string, std::string> >& register_map, const int64_t& resource_id, 
        const std::string& resource_name, file_system_event_aggregator::ChangeDescriptor cd, rsComm_t* _comm, icatSessionStruct *icss, 
        const rodsLong_t& user_id, bool direct_db_access);

void handle_write_fid(const std::vector<std::pair<std::string, std::string> >& register_map, const std::string& physical_path, 
        const std::string& object_identifier, rsComm_t* _comm, icatSessionStruct *icss, bool direct_db_access);



int get_user_id(rsComm_t* _comm, icatSessionStruct *icss, rodsLong_t& user_id, bool direct_db_access_flag);
#endif
