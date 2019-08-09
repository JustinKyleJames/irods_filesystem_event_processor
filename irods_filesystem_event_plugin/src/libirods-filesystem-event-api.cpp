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

// json header
//#include <jeayeson/jeayeson.hpp>

// capn proto
#pragma push_macro("LIST")
#undef LIST

#pragma push_macro("ERROR")
#undef ERROR

#include "../../common/change_table.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#pragma pop_macro("LIST")
#pragma pop_macro("ERROR")

#include "inout_structs.h"
#include "database_routines.hpp"
#include "irods_event_operations.hpp"


int call_irodsBeegfsApiInp_irodsBeegfsApiOut( irods::api_entry* _api, 
                            rsComm_t*  _comm,
                            irodsBeegfsApiInp_t* _inp, 
                            irodsBeegfsApiOut_t** _out ) {
    return _api->call_handler<
               irodsBeegfsApiInp_t*,
               irodsBeegfsApiOut_t** >(
                   _comm,
                   _inp,
                   _out );
}

#ifdef RODS_SERVER
static irods::error serialize_irodsBeegfsApiInp_ptr( boost::any _p, 
                                            irods::re_serialization::serialized_parameter_t& _out) {
    try {
        irodsBeegfsApiInp_t* tmp = boost::any_cast<irodsBeegfsApiInp_t*>(_p);
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
                "failed to cast irodsBeegfsApiInp_t ptr" );
    }

    return SUCCESS();
} // serialize_irodsBeegfsApiInp_ptr

static irods::error serialize_irodsBeegfsApiOut_ptr_ptr( boost::any _p,
                                                irods::re_serialization::serialized_parameter_t& _out) {
    try {
        irodsBeegfsApiOut_t** tmp = boost::any_cast<irodsBeegfsApiOut_t**>(_p);
        if(tmp && *tmp ) {
            irodsBeegfsApiOut_t*  l = *tmp;
            _out["status"] = boost::lexical_cast<std::string>(l->status);
        }
        else {
            _out["status"] = -1;
        }
    }
    catch ( std::exception& ) {
        return ERROR(
                INVALID_ANY_CAST,
                "failed to cast irodsBeegfsApiOut_t ptr" );
    }

    return SUCCESS();
} // serialize_irodsBeegfsApiOut_ptr_ptr
#endif


#ifdef RODS_SERVER
    #define CALL_IRODS_BEEGFS_API_INP_OUT call_irodsBeegfsApiInp_irodsBeegfsApiOut 
#else
    #define CALL_IRODS_BEEGFS_API_INP_OUT NULL 
#endif

// =-=-=-=-=-=-=-
// api function to be referenced by the entry

int rs_handle_records( rsComm_t* _comm, irodsBeegfsApiInp_t* _inp, irodsBeegfsApiOut_t** _out ) {

    rodsLog( LOG_NOTICE, "Dynamic API - Beegfs API" );

    // read the serialized input
    const kj::ArrayPtr<const capnp::word> array_ptr{ reinterpret_cast<const capnp::word*>(&(*(_inp->buf))), 
        reinterpret_cast<const capnp::word*>(&(*(_inp->buf + _inp->buflen)))};
    capnp::FlatArrayMessageReader message(array_ptr);

    ChangeMap::Reader changeMap = message.getRoot<ChangeMap>();
    std::string irods_api_update_type(changeMap.getIrodsApiUpdateType().cStr()); 
    bool direct_db_modification_requested = (irods_api_update_type == "direct");

    // read and populate the register_map which holds a mapping of physical paths to irods paths
    std::vector<std::pair<std::string, std::string> > register_map;
    for (RegisterMapEntry::Reader entry : changeMap.getRegisterMap()) {
        std::string physical_path(entry.getFilePath().cStr());
        std::string irods_register_path(entry.getIrodsRegisterPath().cStr());
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
            rodsLog(LOG_ERROR, "Error:  Attempting bulk Beegfs operations on a catalog consumer.  Must connect to catalog provider.");
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
    ( *_out ) = ( irodsBeegfsApiOut_t* )malloc( sizeof( irodsBeegfsApiOut_t ) );
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

    int64_t resource_id = changeMap.getResourceId();
    std::string resource_name(changeMap.getResourceName().cStr());
    int64_t maximum_records_per_sql_command = changeMap.getMaximumRecordsPerSqlCommand(); 
    bool set_metadata_for_storage_tiering_time_violation = changeMap.getSetMetadataForStorageTieringTimeViolation();
    std::string metadata_key_for_storage_tiering_time_violation = changeMap.getMetadataKeyForStorageTieringTimeViolation();

    // for batched file inserts 
    std::vector<std::string> objectIdentifer_list_for_create;
    std::vector<std::string> physical_path_list;
    std::vector<std::string> object_name_list;
    std::vector<std::string> parent_objectIdentifer_list;
    std::vector<int64_t> file_size_list;

    // for batched file deletes
    std::vector<std::string> objectIdentifer_list_for_unlink;

    for (ChangeDescriptor::Reader entry : changeMap.getEntries()) {

        const ChangeDescriptor::EventTypeEnum event_type = entry.getEventType();
        std::string objectIdentifer(entry.getObjectIdentifier().cStr());
        std::string physical_path(entry.getFilePath().cStr());
        std::string object_name(entry.getObjectName().cStr());
        const ChangeDescriptor::ObjectTypeEnum object_type = entry.getObjectType();
        std::string parent_objectIdentifer(entry.getParentObjectIdentifier().cStr());
        int64_t file_size = entry.getFileSize();

        // Handle changes in iRODS

        if (event_type == ChangeDescriptor::EventTypeEnum::CREATE) {
            if (direct_db_modification_requested) {
                objectIdentifer_list_for_create.push_back(objectIdentifer);
                physical_path_list.push_back(physical_path);
                object_name_list.push_back(object_name);
                parent_objectIdentifer_list.push_back(parent_objectIdentifer);
                file_size_list.push_back(file_size);
            } else {
                handle_create(register_map, resource_id, resource_name,
                        objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                        _comm, icss, user_id, direct_db_modification_requested);
            }
        } else if (event_type == ChangeDescriptor::EventTypeEnum::MKDIR) {
            handle_mkdir(register_map, resource_id, resource_name,
                    objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::OTHER) {
            handle_other(register_map, resource_id, resource_name,
                    objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::RENAME and object_type == ChangeDescriptor::ObjectTypeEnum::FILE) {
            handle_rename_file(register_map, resource_id, resource_name,
                    objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::RENAME and object_type == ChangeDescriptor::ObjectTypeEnum::DIR) {
            handle_rename_dir(register_map, resource_id, resource_name,
                    objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::UNLINK) {
            if (direct_db_modification_requested) {
                objectIdentifer_list_for_unlink.push_back(objectIdentifer);
            } else {
                handle_unlink(register_map, resource_id, resource_name,
                        objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                        _comm, icss, user_id, direct_db_modification_requested);
            }
        } else if (event_type == ChangeDescriptor::EventTypeEnum::RMDIR) {
            handle_rmdir(register_map, resource_id, resource_name,
                    objectIdentifer, physical_path, object_name, object_type, parent_objectIdentifer, file_size,
                    _comm, icss, user_id, direct_db_modification_requested);
        } else if (event_type == ChangeDescriptor::EventTypeEnum::WRITE_FID) {
            handle_write_fid(register_map, physical_path, objectIdentifer, _comm, icss, direct_db_modification_requested);
        }


    }

    if (direct_db_modification_requested) {

        if (objectIdentifer_list_for_unlink.size() > 0) {
            handle_batch_unlink(objectIdentifer_list_for_unlink, resource_id, maximum_records_per_sql_command, _comm, icss);
        }
 
        if (objectIdentifer_list_for_create.size() > 0) {
            handle_batch_create(register_map, resource_id, resource_name,
                    objectIdentifer_list_for_create, physical_path_list, object_name_list, parent_objectIdentifer_list, file_size_list,
                    maximum_records_per_sql_command, _comm, icss, user_id, set_metadata_for_storage_tiering_time_violation,
                    metadata_key_for_storage_tiering_time_violation);
        }
    }

    rodsLog(LOG_NOTICE, "Dynamic Beegfs API - DONE" );

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
                                "IrodsBeegfsApiInp_PI", 0, // in PI / bs flag
                                "IrodsBeegfsApiOut", 0, // out PI / bs flag
                                std::function<
                                    int( rsComm_t*,irodsBeegfsApiInp_t*,irodsBeegfsApiOut_t**)>(
                                        rs_handle_records), // operation
								"rs_handle_records",    // operation name
                                0,  // null clear fcn
                                (funcPtr)CALL_IRODS_BEEGFS_API_INP_OUT
                              };
        // =-=-=-=-=-=-=-
        // create an api object
        irods::api_entry* api = new irods::api_entry( def );

#ifdef RODS_SERVER
        irods::re_serialization::add_operation(
                typeid(irodsBeegfsApiInp_t*),
                serialize_irodsBeegfsApiInp_ptr );

        irods::re_serialization::add_operation(
                typeid(irodsBeegfsApiOut_t**),
                serialize_irodsBeegfsApiOut_ptr_ptr );
#endif // RODS_SERVER

        // =-=-=-=-=-=-=-
        // assign the pack struct key and value
        api->in_pack_key   = "IrodsBeegfsApiInp_PI";
        api->in_pack_value = IrodsBeegfsApiInp_PI;

        api->out_pack_key   = "IrodsBeegfsApiOut_PI";
        api->out_pack_value = IrodsBeegfsApiOut_PI;

        return api;

    } // plugin_factory

}; // extern "C"
