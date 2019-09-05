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

#if defined(COCKROACHDB_ICAT)
  #include "mid_level_cockroachdb.hpp"
  #include "low_level_cockroachdb.hpp"
#else
  #include "mid_level_other.hpp"
  #include "low_level_odbc_other.hpp"
#endif

#include "boost/lexical_cast.hpp"
#include "boost/filesystem.hpp"

#include "database_routines.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <sstream>
#include <string>
#include <iostream>
#include <vector>

// avro
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "../../common/change_table_avro.hpp"

#include "inout_structs.h"
#include "database_routines.hpp"
#include "irods_event_operations.hpp"


int call_irodsFsEventApiInp_irodsFsEventApiOut( irods::api_entry* _api, 
                            rsComm_t*  _comm,
                            irodsFsEventApiInp_t* _inp, 
                            irodsFsEventApiOut_t** _out ) {
    return _api->call_handler<
               irodsFsEventApiInp_t*,
               irodsFsEventApiOut_t** >(
                   _comm,
                   _inp,
                   _out );
}

#ifdef RODS_SERVER
static irods::error serialize_irodsFsEventApiInp_ptr( boost::any _p, 
                                            irods::re_serialization::serialized_parameter_t& _out) {
    try {
        irodsFsEventApiInp_t* tmp = boost::any_cast<irodsFsEventApiInp_t*>(_p);
        if(tmp) {
            _out["buf"] = boost::lexical_cast<std::string>(tmp->buf);
        }
        else {
            _out["buf"] = "";
        }
    }
    catch ( std::exception& ) {
        return ERROR(
                INVALID_ANY_CAST,
                "failed to cast irodsFsEventApiInp_t ptr" );
    }

    return SUCCESS();
} // serialize_irodsFsEventApiInp_ptr

static irods::error serialize_irodsFsEventApiOut_ptr_ptr( boost::any _p,
                                                irods::re_serialization::serialized_parameter_t& _out) {
    try {
        irodsFsEventApiOut_t** tmp = boost::any_cast<irodsFsEventApiOut_t**>(_p);
        if(tmp && *tmp ) {
            irodsFsEventApiOut_t*  l = *tmp;
            _out["status"] = boost::lexical_cast<std::string>(l->status);
        }
        else {
            _out["status"] = -1;
        }
    }
    catch ( std::exception& ) {
        return ERROR(
                INVALID_ANY_CAST,
                "failed to cast irodsFsEventApiOut_t ptr" );
    }

    return SUCCESS();
} // serialize_irodsFsEventApiOut_ptr_ptr
#endif


#ifdef RODS_SERVER
    #define CALL_IRODS_FS_EVENT_API_INP_OUT call_irodsFsEventApiInp_irodsFsEventApiOut 
#else
    #define CALL_IRODS_FS_EVENT_API_INP_OUT NULL 
#endif

// =-=-=-=-=-=-=-
// api function to be referenced by the entry

int rs_handle_records( rsComm_t* _comm, irodsFsEventApiInp_t* _inp, irodsFsEventApiOut_t** _out ) {

    rodsLog( LOG_NOTICE, "Dynamic API - File System Event Handler API" );

    // read the serialized input
    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(
            static_cast<const uint8_t*>(_inp->buf), _inp->buflen);

    avro::DecoderPtr dec = avro::binaryDecoder();
    dec->init(*in);
    file_system_event_aggregator::ChangeMap changeMap; 
    avro::decode(*dec, changeMap);

    std::string irods_api_update_type(changeMap.irodsApiUpdateType); 
    bool direct_db_modification_requested = (irods_api_update_type == "direct");

    // read and populate the register_map which holds a mapping of physical paths to irods paths
    std::vector<std::pair<std::string, std::string> > register_map;
    for (auto entry : changeMap.registerMap) {
        std::string physical_path(entry.physical_path);
        std::string irods_register_path(entry.irodsRegisterPath);
        register_map.push_back(std::make_pair(physical_path, irods_register_path));
    }


    int status;
    icatSessionStruct *icss = nullptr;

    // Bulk request must be performed on an iCAT server if doing direct DB access.  If this is not the iCAT, 
    // forward this request to it.
   
    // Because the directory rename must be done with a direct db access **for now**, even when policy is 
    // enabled, we need a connection to the DB.
    // if (direct_db_modification_requested) {

        rodsServerHost_t *rodsServerHost;
        status = getAndConnRcatHost(_comm, MASTER_RCAT, (const char*)nullptr, &rodsServerHost);
        if ( status < 0 ) {
            rodsLog(LOG_ERROR, "Error:  getAndConnRcatHost returned %d", status);
            return status;
        }

        if ( rodsServerHost->localFlag != LOCAL_HOST ) {
            rodsLog(LOG_NOTICE, "Bulk request received by catalog consumer.  Forwarding request to catalog provider.");
            status = procApiRequest(rodsServerHost->conn, 15001, _inp, nullptr, (void**)_out, nullptr);
            return status;
        }

        std::string svc_role;
        irods::error ret = get_catalog_service_role(svc_role);
        if(!ret.ok()) {
            irods::log(PASS(ret));
            return ret.code();
        }

        if (irods::CFG_SERVICE_ROLE_PROVIDER != svc_role) {
            rodsLog(LOG_ERROR, "Error:  Attempting bulk operations on a catalog consumer.  Must connect to catalog provider.");
            return CAT_NOT_OPEN;
        }

        status = chlGetRcs( &icss );
        if ( status < 0 || !icss ) {
            return CAT_NOT_OPEN;
        }

#if MY_ICAT
        // for mysql, lower the isolation level for the large updates to 
        // avoid deadlocks 
        setMysqlIsolationLevelReadCommitted(icss);
#endif
//    }

    // setup the output struct
    ( *_out ) = ( irodsFsEventApiOut_t* )malloc( sizeof( irodsFsEventApiOut_t ) );
    ( *_out )->status = 0;

    rodsLong_t user_id;

    // if we are using direct db access, we need to get the user_name from the user_id
    if (direct_db_modification_requested) {
        status = get_user_id(_comm, icss, user_id, direct_db_modification_requested);
        if (status != 0) {
           rodsLog(LOG_ERROR, "Error getting user_id for user %s.  Error is %i", _comm->clientUser.userName, status);
           return SYS_USER_RETRIEVE_ERR;
        }
    }

    int64_t resource_id = changeMap.resourceId;
    std::string resource_name(changeMap.resourceName);
    int64_t maximum_records_per_sql_command = changeMap.maximumRecordsPerSqlCommand; 
    bool set_metadata_for_storage_tiering_time_violation = changeMap.setMetadataForStorageTieringTimeViolation;
    std::string metadata_key_for_storage_tiering_time_violation = changeMap.metadataKeyForStorageTieringTimeViolation;

    // for batched file inserts 
    std::vector<std::string> object_identifier_list_for_create;
    std::vector<std::string> physical_path_list;
    std::vector<std::string> object_name_list;
    std::vector<std::string> target_parent_object_identifier_list;
    std::vector<int64_t> file_size_list;

    // for batched file deletes
    std::vector<std::string> object_identifier_list_for_unlink;

    for (auto entry : changeMap.entries) {

        // Handle changes in iRODS

        if (entry.event_type == file_system_event_aggregator::EventTypeEnum::CREATE) {
            if (direct_db_modification_requested) {
                object_identifier_list_for_create.push_back(entry.object_identifier);
                physical_path_list.push_back(entry.target_physical_path);
                object_name_list.push_back(entry.object_name);
                target_parent_object_identifier_list.push_back(entry.target_parent_object_identifier);
                file_size_list.push_back(entry.file_size);
            } else {
                handle_create(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
            }
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::MKDIR) {
            handle_mkdir(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::OTHER) {
            handle_other(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::RENAME && 
                   entry.object_type == file_system_event_aggregator::ObjectTypeEnum::FILE) {
            handle_rename_file(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::RENAME && 
                   entry.object_type == file_system_event_aggregator::ObjectTypeEnum::DIR) {
            handle_rename_dir(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::UNLINK) {
            if (direct_db_modification_requested) {
                object_identifier_list_for_unlink.push_back(entry.object_identifier);
            } else {
                handle_unlink(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
            }
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::RMDIR) {
            handle_rmdir(register_map, resource_id, resource_name, entry, _comm, icss, user_id, direct_db_modification_requested);
        } else if (entry.event_type == file_system_event_aggregator::EventTypeEnum::WRITE_FID) {
            handle_write_fid(register_map, entry.target_physical_path, entry.object_identifier, _comm, icss, direct_db_modification_requested);
        }


    }

    if (direct_db_modification_requested) {

        if (object_identifier_list_for_unlink.size() > 0) {
            handle_batch_unlink(object_identifier_list_for_unlink, resource_id, maximum_records_per_sql_command, _comm, icss);
        }
 
        if (object_identifier_list_for_create.size() > 0) {
            handle_batch_create(register_map, resource_id, resource_name,
                    object_identifier_list_for_create, physical_path_list, object_name_list, target_parent_object_identifier_list, file_size_list,
                    maximum_records_per_sql_command, _comm, icss, user_id, set_metadata_for_storage_tiering_time_violation,
                    metadata_key_for_storage_tiering_time_violation);
        }
    }

    rodsLog(LOG_NOTICE, "Dynamic File System Event Handler API - DONE" );

    return 0;
}


extern "C" {
    // =-=-=-=-=-=-=-
    // factory function to provide instance of the plugin
    irods::api_entry* plugin_factory( const std::string&,     //_inst_name
                                      const std::string& ) { // _context
        // =-=-=-=-=-=-=-
        // create a api def object
        irods::apidef_t def = { 15001,             // api number
                                RODS_API_VERSION, // api version
                                NO_USER_AUTH,     // client auth
                                NO_USER_AUTH,     // proxy auth
                                "IrodsFsEventApiInp_PI", 0, // in PI / bs flag
                                "IrodsFsEventApiOut_PI", 0, // out PI / bs flag
                                std::function<
                                    int( rsComm_t*,irodsFsEventApiInp_t*,irodsFsEventApiOut_t**)>(
                                        rs_handle_records), // operation
								"rs_handle_records",    // operation name
                                0,  // null clear fcn
                                (funcPtr)CALL_IRODS_FS_EVENT_API_INP_OUT
                              };
        // =-=-=-=-=-=-=-
        // create an api object
        irods::api_entry* api = new irods::api_entry( def );

#ifdef RODS_SERVER
        irods::re_serialization::add_operation(
                typeid(irodsFsEventApiInp_t*),
                serialize_irodsFsEventApiInp_ptr );

        irods::re_serialization::add_operation(
                typeid(irodsFsEventApiOut_t**),
                serialize_irodsFsEventApiOut_ptr_ptr );
#endif // RODS_SERVER

        // =-=-=-=-=-=-=-
        // assign the pack struct key and value
        api->in_pack_key   = "IrodsFsEventApiInp_PI";
        api->in_pack_value = IrodsFsEventApiInp_PI;

        api->out_pack_key   = "IrodsFsEventApiOut_PI";
        api->out_pack_value = IrodsFsEventApiOut_PI;

        return api;

    } // plugin_factory

}; // extern "C"
