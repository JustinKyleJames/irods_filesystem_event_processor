#include <string>
#include <set>
#include <ctime>
#include <sstream>
#include <mutex>
#include <thread>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


// boost headers
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

// avro
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"


// local headers
#include "change_table.hpp"
#include "inout_structs.h"
#include "logging.hpp"
#include "config.hpp"

// common headers
#include "../../common/irods_filesystem_event_processor_errors.hpp"
#include "../../common/change_table_avro.hpp"

// sqlite
#include <sqlite3.h>

std::string event_type_to_str(file_system_event_aggregator::EventTypeEnum type);
std::string object_type_to_str(file_system_event_aggregator::ObjectTypeEnum type);


//using namespace boost::interprocess;

//static boost::shared_mutex change_table_mutex;
static std::mutex change_table_mutex;

size_t get_change_table_size(change_map_t& change_map) {
    std::lock_guard<std::mutex> lock(change_table_mutex);
    return change_map.size();
}
    

int write_objectId_to_root_dir(const std::string& fs_mount_path, const std::string& objectId, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    change_descriptor entry{};
    entry.cr_index = 0;
    entry.objectId = objectId;
    entry.parent_objectId = "";
    entry.object_name = "";
    entry.object_type = file_system_event_aggregator::ObjectTypeEnum::DIR;
    entry.physical_path = fs_mount_path;
    entry.oper_complete = true;
    entry.timestamp = time(NULL);
    entry.last_event = file_system_event_aggregator::EventTypeEnum::WRITE_FID;
    change_map.insert(entry);

    return irods_filesystem_event_processor_error::SUCCESS;

}

int handle_close(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                 const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);
 
    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    struct stat st;
    int result = stat(physical_path.c_str(), &st);

    LOG(LOG_DBG, "stat(%s, &st)", physical_path.c_str());
    LOG(LOG_DBG, "handle_close:  stat_result = %i, file_size = %ld", result, st.st_size);

    auto iter = change_map_objectId.find(objectId);
    if (change_map_objectId.end() != iter) {
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (0 == result) {
            change_map_objectId.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
        }
    } else {
        // this is probably an append so no file update is done
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        entry.parent_objectId = parent_objectId;
        entry.object_name = object_name;
        entry.object_type = (result == 0 && S_ISDIR(st.st_mode)) ? file_system_event_aggregator::ObjectTypeEnum::DIR : file_system_event_aggregator::ObjectTypeEnum::FILE;
        entry.physical_path = physical_path; 
        entry.oper_complete = true;
        entry.timestamp = time(NULL);
        entry.last_event = file_system_event_aggregator::EventTypeEnum::OTHER;
        if (0 == result) {
            entry.file_size = st.st_size;
        }

        change_map.insert(entry);
    }
    return irods_filesystem_event_processor_error::SUCCESS;

}

int handle_mkdir(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                 const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {

    LOG(LOG_ERR, "handle_mkdir:  parent_objectId=%s", parent_objectId.c_str());

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    auto iter = change_map_objectId.find(objectId);
    if(iter != change_map_objectId.end()) {
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.last_event = file_system_event_aggregator::EventTypeEnum::MKDIR; });
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        entry.parent_objectId = parent_objectId;
        entry.object_name = object_name;
        entry.physical_path = physical_path;
        entry.oper_complete = true;
        entry.last_event = file_system_event_aggregator::EventTypeEnum::MKDIR;
        entry.timestamp = time(NULL);
        entry.object_type = file_system_event_aggregator::ObjectTypeEnum::DIR;
        change_map.insert(entry);
    }
    return irods_filesystem_event_processor_error::SUCCESS; 

}

int handle_rmdir(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                 const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();


    auto iter = change_map_objectId.find(objectId);
    if(iter != change_map_objectId.end()) {
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [parent_objectId](change_descriptor &cd){ cd.parent_objectId = parent_objectId; });
        change_map_objectId.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map_objectId.modify(iter, [physical_path](change_descriptor &cd){ cd.physical_path = physical_path; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.last_event = file_system_event_aggregator::EventTypeEnum::RMDIR; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        entry.oper_complete = true;
        entry.last_event = file_system_event_aggregator::EventTypeEnum::RMDIR;
        entry.timestamp = time(NULL);
        entry.object_type = file_system_event_aggregator::ObjectTypeEnum::DIR;
        entry.parent_objectId = parent_objectId;
        entry.object_name = object_name;
        change_map.insert(entry);
    }
    return irods_filesystem_event_processor_error::SUCCESS; 


}

int handle_unlink(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                  const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {
  
    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();


    auto iter = change_map_objectId.find(objectId);
    if(iter != change_map_objectId.end()) {   

        // If an add and a delete occur in the same transactional unit, just delete the transaction
        if (file_system_event_aggregator::EventTypeEnum::CREATE == iter->last_event) {
            change_map_objectId.erase(iter);
        } else {
            change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
            change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = true; });
            change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.last_event = file_system_event_aggregator::EventTypeEnum::UNLINK; });
            change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
       }
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        entry.parent_objectId = parent_objectId;
        entry.physical_path = physical_path;
        entry.oper_complete = true;
        entry.last_event = file_system_event_aggregator::EventTypeEnum::UNLINK;
        entry.timestamp = time(NULL);
        entry.object_type = file_system_event_aggregator::ObjectTypeEnum::FILE;
        entry.object_name = object_name;
        change_map.insert(entry);
    }

    return irods_filesystem_event_processor_error::SUCCESS; 
}

int handle_rename(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                  const std::string& object_name, const std::string& physical_path, const std::string& old_physical_path, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    auto iter = change_map_objectId.find(objectId);
    std::string original_path;

    struct stat statbuf;
    bool is_dir = stat(physical_path.c_str(), &statbuf) == 0 && S_ISDIR(statbuf.st_mode);

    // if there is a previous entry, just update the physical_path to the new path
    // otherwise, add a new entry
    if(iter != change_map_objectId.end()) {
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [parent_objectId](change_descriptor &cd){ cd.parent_objectId = parent_objectId; });
        change_map_objectId.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map_objectId.modify(iter, [physical_path](change_descriptor &cd){ cd.physical_path = physical_path; });
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        entry.parent_objectId = parent_objectId;
        entry.object_name = object_name;
        entry.physical_path = physical_path;
        entry.oper_complete = true;
        entry.last_event = file_system_event_aggregator::EventTypeEnum::RENAME;
        entry.timestamp = time(NULL);
        if (is_dir) {
            entry.object_type = file_system_event_aggregator::ObjectTypeEnum::DIR;
        } else  {
            entry.object_type = file_system_event_aggregator::ObjectTypeEnum::FILE;
        }
        /*if (is_dir) {
            change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.object_type = file_system_event_aggregator::ObjectTypeEnum::DIR; });
        } else {
            change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.object_type = file_system_event_aggregator::ObjectTypeEnum::FILE; });
        }*/
        change_map.insert(entry);
    }

    LOG(LOG_DBG, "rename:  old_physical_path = %s", old_physical_path.c_str());

    if (is_dir) {

        // search through and update all references in table
        for (auto iter = change_map_objectId.begin(); iter != change_map_objectId.end(); ++iter) {
            std::string p = iter->physical_path;
            if (p.length() > 0 && p.length() != old_physical_path.length() && boost::starts_with(p, old_physical_path)) {
                change_map_objectId.modify(iter, [old_physical_path, physical_path](change_descriptor &cd){ cd.physical_path.replace(0, old_physical_path.length(), physical_path); });
            }
        }
    }
    return irods_filesystem_event_processor_error::SUCCESS; 


}

int handle_create(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                  const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    auto iter = change_map_objectId.find(objectId);
    if(iter != change_map_objectId.end()) {
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [parent_objectId](change_descriptor &cd){ cd.parent_objectId = parent_objectId; });
        change_map_objectId.modify(iter, [object_name](change_descriptor &cd){ cd.object_name = object_name; });
        change_map_objectId.modify(iter, [physical_path](change_descriptor &cd){ cd.physical_path = physical_path; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.last_event = file_system_event_aggregator::EventTypeEnum::CREATE; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        entry.parent_objectId = parent_objectId;
        entry.object_name = object_name;
        entry.physical_path = physical_path;
        entry.oper_complete = false;
        entry.last_event = file_system_event_aggregator::EventTypeEnum::CREATE;
        entry.timestamp = time(NULL);
        entry.object_type = file_system_event_aggregator::ObjectTypeEnum::FILE;
        change_map.insert(entry);
    }

    return irods_filesystem_event_processor_error::SUCCESS; 

}

int handle_mtime(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                 const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    auto iter = change_map_objectId.find(objectId);
    if(iter != change_map_objectId.end()) {   
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        //entry.parent_objectId = parent_objectId;
        //entry.physical_path = physical_path;
        //entry.object_name = object_name;
        entry.last_event = file_system_event_aggregator::EventTypeEnum::OTHER;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        change_map.insert(entry);
    }

    return irods_filesystem_event_processor_error::SUCCESS; 

}

int handle_trunc(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                 const std::string& object_name, const std::string& physical_path, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with hashed index of objectId
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    struct stat st;
    int result = stat(physical_path.c_str(), &st);

    LOG(LOG_DBG, "handle_trunc:  stat_result = %i, file_size = %ld", result, st.st_size);

    auto iter = change_map_objectId.find(objectId);
    if(iter != change_map_objectId.end()) {
        change_map_objectId.modify(iter, [cr_index](change_descriptor &cd){ cd.cr_index = cr_index; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.oper_complete = false; });
        change_map_objectId.modify(iter, [](change_descriptor &cd){ cd.timestamp = time(NULL); });
        if (0 == result) {
            change_map_objectId.modify(iter, [st](change_descriptor &cd){ cd.file_size = st.st_size; });
        }
    } else {
        change_descriptor entry{};
        entry.cr_index = cr_index;
        entry.objectId = objectId;
        //entry.parent_objectId = parent_objectId;
        //entry.physical_path = physical_path;
        //entry.object_name = object_name;
        entry.oper_complete = false;
        entry.timestamp = time(NULL);
        if (0 == result) {
            entry.file_size = st.st_size;
        }
        change_map.insert(entry);
    }

    return irods_filesystem_event_processor_error::SUCCESS; 


}

int remove_objectId_from_table(const std::string& objectId, change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with index of objectId 
    auto &change_map_objectId = change_map.get<change_descriptor_objectId_idx>();

    change_map_objectId.erase(objectId);

    return irods_filesystem_event_processor_error::SUCCESS;
}

// This is just a debugging function
void write_change_table_to_str(const change_map_t& change_map, std::string& buffer) {

    boost::format change_record_header_format_obj("\n%-15s %-30s %-30s %-12s %-20s %-30s %-17s %-11s %-16s %-10s\n");
    boost::format change_record_format_obj("%015u %-30s %-30s %-12s %-20s %-30s %-17s %-11s %-16s %lu\n");

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map_seq = change_map.get<change_descriptor_seq_idx>();

    char time_str[18];

    buffer = str(change_record_header_format_obj % "CR_INDEX" % "FIDSTR" % "PARENT_FIDSTR" % "OBJECT_TYPE" % "OBJECT_NAME" % "PHYSICAL_PATH" % "TIME" %
            "EVENT_TYPE" % "OPER_COMPLETE?" % "FILE_SIZE");

    buffer += str(change_record_header_format_obj % "--------" % "------" % "-------------" % "-----------"% "-----------" % "-------------" % "----" % 
            "----------" % "--------------" % "---------");

    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {
         std::string objectId = iter->objectId;

         struct tm *timeinfo;
         timeinfo = localtime(&iter->timestamp);
         strftime(time_str, sizeof(time_str), "%Y%m%d %I:%M:%S", timeinfo);

         buffer += str(change_record_format_obj % iter->cr_index % objectId.c_str() % iter->parent_objectId.c_str() %
                 object_type_to_str(iter->object_type).c_str() %
                 iter->object_name.c_str() % 
                 iter->physical_path.c_str() % time_str % 
                 event_type_to_str(iter->last_event).c_str() %
                 (iter->oper_complete == 1 ? "true" : "false") % iter->file_size);

    }

}

// This is just a debugging function
void print_change_table(const change_map_t& change_map) {
   
    std::string change_table_str; 
    write_change_table_to_str(change_map, change_table_str);
    LOG(LOG_DBG, "%s", change_table_str.c_str());
}

// Sets the update status.  
//
// TODO:  This does a decode and encode.  
int set_update_status_in_avro_buf(const boost::shared_ptr< std::vector<uint8_t>>& old_buffer, const std::string& update_status, 
        boost::shared_ptr< std::vector<uint8_t>>& new_buffer) {

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(old_buffer->data(), old_buffer->size());
    avro::DecoderPtr dec = avro::binaryDecoder();
    dec->init(*in);
    file_system_event_aggregator::ChangeMap map; 
    avro::decode(*dec, map);

    map.updateStatus = update_status;

    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::encode(*e, map);
    new_buffer = avro::snapshot( *out );
     
    return irods_filesystem_event_processor_error::SUCCESS;
}


//int get_update_status_from_avro_buf(const boost::shared_ptr< std::vector< uint8_t > >& data, std::string& update_status) {
int get_update_status_from_avro_buf(const unsigned char* buf, const size_t buflen, std::string& update_status) {

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(
            static_cast<const uint8_t*>(buf), buflen);

    avro::DecoderPtr dec = avro::binaryDecoder();
    dec->init(*in);
    file_system_event_aggregator::ChangeMap map; 
    avro::decode(*dec, map);

    update_status = map.updateStatus;
     
    return irods_filesystem_event_processor_error::SUCCESS;
}


// Processes change table by writing records ready to be sent to iRODS into avro buffer (buffer).
int write_change_table_to_avro_buf(const filesystem_event_aggregator_cfg_t *config_struct_ptr, boost::shared_ptr< std::vector<uint8_t>>& buffer, 
        change_map_t& change_map, std::multiset<std::string>& active_objectId_list) {

    // store up a list of objectId that are being added to this buffer
    std::multiset<std::string> temp_objectId_list;

    if (nullptr == config_struct_ptr) {
        LOG(LOG_ERR, "Null config_struct_ptr sent to %s - %d", __FUNCTION__, __LINE__);
        return irods_filesystem_event_processor_error::INVALID_OPERAND_ERROR;
    }

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map with sequenced index  
    auto &change_map_seq = change_map.get<change_descriptor_seq_idx>();

    // initialize avro message
    
    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);

    file_system_event_aggregator::ChangeMap map;

    map.resourceId = config_struct_ptr->irods_resource_id;
    map.resourceName = config_struct_ptr->irods_resource_name;
    map.updateStatus = "PENDING";
    map.irodsApiUpdateType = config_struct_ptr->irods_api_update_type;
    map.maximumRecordsPerSqlCommand = config_struct_ptr->maximum_records_per_sql_command;
    map.setMetadataForStorageTieringTimeViolation = config_struct_ptr->set_metadata_for_storage_tiering_time_violation;
    map.metadataKeyForStorageTieringTimeViolation = config_struct_ptr->metadata_key_for_storage_tiering_time_violation;

    // populate the register map
    for (auto& iter : config_struct_ptr->register_map) {
        file_system_event_aggregator::RegisterMapEntry entry;
        entry.filePath = iter.first;
        entry.irodsRegisterPath = iter.second;
        map.registerMap.push_back(entry);
    }

    // populate the change entries
    size_t write_count = change_map_seq.size() >= config_struct_ptr->maximum_records_per_update_to_irods 
        ? config_struct_ptr->maximum_records_per_update_to_irods : change_map_seq.size() ;

    bool collision_in_objectId = false;
    size_t cnt = 0;
    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end() && cnt < write_count;) { 

        LOG(LOG_DBG, "objectId=%s oper_complete=%i", iter->objectId.c_str(), iter->oper_complete);

        LOG(LOG_DBG, "change_map size = %lu", change_map_seq.size()); 

        if (iter->oper_complete) {

            // break out of the main loop if we reach a point where a previous event needs to be acted upon first. 
            // For MKDIR, CREATE, and RENAME - break out if there is an active event on the parent.
            // For RMDIR, UNLINK - break out of there is an active event on itself

            if (iter->last_event == file_system_event_aggregator::EventTypeEnum::MKDIR ||
                    iter->last_event == file_system_event_aggregator::EventTypeEnum::CREATE ||
                    iter->last_event == file_system_event_aggregator::EventTypeEnum::RENAME) {

                if (active_objectId_list.find(iter->parent_objectId) != active_objectId_list.end()) {
                    LOG(LOG_DBG, "objectId %s is already in active objectId list - breaking out ", iter->parent_objectId.c_str());
                    collision_in_objectId = true;
                    break;
                }

            } else if (iter->last_event == file_system_event_aggregator::EventTypeEnum::RMDIR ||
                    iter->last_event == file_system_event_aggregator::EventTypeEnum::UNLINK) {

                if (active_objectId_list.find(iter->objectId) != active_objectId_list.end()) {
                    LOG(LOG_DBG, "objectId %s is already in active objectId list - breaking out", iter->objectId.c_str());
                    collision_in_objectId = true;
                    break;
                }
            }

          
            // if MKDIR, CREATE, or RENAME put self on active list, for RMDIR and UNLINK put parent on active list
            if (iter->last_event == file_system_event_aggregator::EventTypeEnum::MKDIR ||
                    iter->last_event == file_system_event_aggregator::EventTypeEnum::CREATE ||
                    iter->last_event == file_system_event_aggregator::EventTypeEnum::RENAME) {

                LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: Adding SELF %s to active objectId list", iter->objectId.c_str());
                temp_objectId_list.insert(iter->objectId);

            } else if (iter->last_event == file_system_event_aggregator::EventTypeEnum::RMDIR ||
                    iter->last_event == file_system_event_aggregator::EventTypeEnum::UNLINK) {
                
                LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: Adding PARENT %s to active objectId list", iter->parent_objectId.c_str());
                temp_objectId_list.insert(iter->parent_objectId);
            }

            // populate the change_entry and push it onto the map
            file_system_event_aggregator::ChangeDescriptor change_entry;            
            change_entry.crIndex = iter->cr_index;
            change_entry.objectIdentifier = iter->objectId;
            change_entry.parentObjectIdentifier = iter->parent_objectId;
            change_entry.objectType = iter->object_type;
            change_entry.objectName = iter->object_name;
            change_entry.filePath = iter->physical_path;
            change_entry.eventType = iter->last_event;
            change_entry.fileSize= iter->file_size;


            // **** debug **** 
            LOG(LOG_DBG, "Entry: [objectId=%s][parent_objectId=%s][object_name=%s][physical_path=%s][file_size=%d]", change_entry.objectIdentifier.c_str(), 
                    change_entry.parentObjectIdentifier.c_str(), change_entry.objectName.c_str(), change_entry.filePath.c_str(), change_entry.fileSize);
            // *************
            
            map.entries.push_back(change_entry);

            // delete entry from table 
            iter = change_map_seq.erase(iter);

            ++cnt;

            LOG(LOG_DBG, "after erase change_map size = %lu", change_map_seq.size());

        } else {
            ++iter;
        }
        
    }

    LOG(LOG_DBG, "write_count=%lu cnt=%lu", write_count, cnt);

    avro::encode(*e, map);
    buffer = avro::snapshot( *out );

    LOG(LOG_DBG, "message_size=%lu", buffer->size());

    // add all fid strings from temp_objectId to active_objectId_list
    active_objectId_list.insert(temp_objectId_list.begin(), temp_objectId_list.end());

    if (collision_in_objectId) {
        return irods_filesystem_event_processor_error::COLLISION_IN_FIDSTR;
    }

    return irods_filesystem_event_processor_error::SUCCESS;
}

// If we get a failure, the accumulator needs to add the entry back to the list.
int add_avro_buffer_back_to_change_table(const boost::shared_ptr< std::vector<uint8_t>>& buffer, change_map_t& change_map, 
        std::multiset<std::string>& active_objectId_list) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(buffer->data(), buffer->size());
    avro::DecoderPtr dec = avro::binaryDecoder();
    dec->init(*in);
    file_system_event_aggregator::ChangeMap map; 
    avro::decode(*dec, map);

    for (auto iter = map.entries.begin(); iter != map.entries.end(); ++iter) {

        change_descriptor record {};

        record.cr_index = iter->crIndex;
        record.last_event = iter->eventType;
        record.objectId = iter->objectIdentifier;
        record.physical_path = iter->filePath;
        record.object_name = iter->objectName;
        record.object_type = iter->objectType;
        record.parent_objectId = iter->parentObjectIdentifier; 
        record.file_size = iter->fileSize;;
        record.oper_complete = true;
        record.timestamp = time(NULL);
   
        LOG(LOG_DBG, "writing entry back to change_map.");

        change_map.insert(record);

        // update active object id list
        if (iter->eventType == file_system_event_aggregator::EventTypeEnum::MKDIR ||
                iter->eventType == file_system_event_aggregator::EventTypeEnum::CREATE ||
                iter->eventType == file_system_event_aggregator::EventTypeEnum::RENAME) {

            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: Removing one copy of SELF %s from active_objectId_list", iter->objectIdentifier.c_str());
            auto itr = active_objectId_list.find(iter->objectIdentifier);
            if(itr != active_objectId_list.end()){
                active_objectId_list.erase(itr);
            }
            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: active_objectId_list size = %lu", active_objectId_list.size());

        } else if (iter->eventType == file_system_event_aggregator::EventTypeEnum::RMDIR ||
                iter->eventType == file_system_event_aggregator::EventTypeEnum::UNLINK ) {

            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: Removing one copy of PARENT %s from active_objectId_list", iter->parentObjectIdentifier.c_str());
            auto itr = active_objectId_list.find(iter->parentObjectIdentifier);
            if(itr != active_objectId_list.end()){
                active_objectId_list.erase(itr);
            }
            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: active_objectId_list size = %lu", active_objectId_list.size());
        }
    }

    return irods_filesystem_event_processor_error::SUCCESS;
}   

void remove_objectIds_in_avro_buffer_from_active_list(const boost::shared_ptr< std::vector<uint8_t>>& buffer, std::multiset<std::string>& active_objectId_list) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(buffer->data(), buffer->size());
    avro::DecoderPtr dec = avro::binaryDecoder();
    dec->init(*in);
    file_system_event_aggregator::ChangeMap map; 
    avro::decode(*dec, map);

    for (auto entry : map.entries) {

        if (entry.eventType == file_system_event_aggregator::EventTypeEnum::MKDIR ||
                entry.eventType == file_system_event_aggregator::EventTypeEnum::CREATE ||
                entry.eventType == file_system_event_aggregator::EventTypeEnum::RENAME) {

            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: Removing one copy of SELF %s from active_objectId_list", entry.objectIdentifier.c_str());
            auto itr = active_objectId_list.find(entry.objectIdentifier);
            if(itr != active_objectId_list.end()){
                active_objectId_list.erase(itr);
            }
            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: active_objectId_list size = %lu", active_objectId_list.size());

        } else if (entry.eventType == file_system_event_aggregator::EventTypeEnum::RMDIR ||
                entry.eventType == file_system_event_aggregator::EventTypeEnum::UNLINK ) {

            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: Removing one copy of PARENT %s from active_objectId_list", entry.parentObjectIdentifier.c_str());
            auto itr = active_objectId_list.find(entry.parentObjectIdentifier);
            if(itr != active_objectId_list.end()){
                active_objectId_list.erase(itr);
            }
            LOG(LOG_DBG, "ACTIVE_OBJECTID_LIST: active_objectId_list size = %lu", active_objectId_list.size());

        }
    }
}


std::string event_type_to_str(file_system_event_aggregator::EventTypeEnum type) {
    switch (type) {
        case file_system_event_aggregator::EventTypeEnum::OTHER:
            return "OTHER";
            break;
        case file_system_event_aggregator::EventTypeEnum::CREATE:
            return "CREATE";
            break;
        case file_system_event_aggregator::EventTypeEnum::UNLINK:
            return "UNLINK";
            break;
        case file_system_event_aggregator::EventTypeEnum::RMDIR:
            return "RMDIR";
            break;
        case file_system_event_aggregator::EventTypeEnum::MKDIR:
            return "MKDIR";
            break;
        case file_system_event_aggregator::EventTypeEnum::RENAME:
            return "RENAME";
            break;
        case file_system_event_aggregator::EventTypeEnum::WRITE_FID:
            return "WRITE_FID";
            break;

    }
    return "";
}

file_system_event_aggregator::EventTypeEnum str_to_event_type(const std::string& str) {
    if ("CREATE" == str) {
        return file_system_event_aggregator::EventTypeEnum::CREATE;
    } else if ("UNLINK" == str) {
        return file_system_event_aggregator::EventTypeEnum::UNLINK;
    } else if ("RMDIR" == str) {
        return file_system_event_aggregator::EventTypeEnum::RMDIR;
    } else if ("MKDIR" == str) {
        return file_system_event_aggregator::EventTypeEnum::MKDIR;
    } else if ("RENAME" == str) {
        return file_system_event_aggregator::EventTypeEnum::RENAME;
    } else if ("WRITE_FID" == str) {
        return file_system_event_aggregator::EventTypeEnum::WRITE_FID;
    }
    return file_system_event_aggregator::EventTypeEnum::OTHER;
}

std::string object_type_to_str(file_system_event_aggregator::ObjectTypeEnum type) {
    switch (type) {
        case file_system_event_aggregator::ObjectTypeEnum::FILE: 
            return "FILE";
            break;
        case file_system_event_aggregator::ObjectTypeEnum::DIR:
            return "DIR";
            break;
    }
    return "";
}

file_system_event_aggregator::ObjectTypeEnum str_to_object_type(const std::string& str) {
    if ("DIR" == str)  {
        return file_system_event_aggregator::ObjectTypeEnum::DIR;
   }
   return file_system_event_aggregator::ObjectTypeEnum::FILE;
} 

bool entries_ready_to_process(change_map_t& change_map) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    // get change map indexed on oper_complete 
    auto &change_map_oper_complete = change_map.get<change_descriptor_oper_complete_idx>();
    bool ready = change_map_oper_complete.count(true) > 0;
    LOG(LOG_DBG, "change map size = %lu", change_map.size());
    LOG(LOG_DBG, "entries_ready_to_process = %i", ready);
    return ready; 
}

int serialize_change_map_to_sqlite(change_map_t& change_map, const std::string& db_file) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    sqlite3 *db;
    int rc;

    std::string serialize_file = db_file + ".db";

    rc = sqlite3_open(serialize_file.c_str(), &db);

    if (rc) {
        LOG(LOG_ERR, "Can't open %s for serialization.", serialize_file.c_str());
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    // get change map with sequenced index  
    auto &change_map_seq = change_map.get<change_descriptor_seq_idx>();

    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {  

        // don't serialize the event that adds the fid to the root directory as this gets generated 
        // every time on restart
        if (iter->last_event == file_system_event_aggregator::EventTypeEnum::WRITE_FID) {
            continue;
        }

        sqlite3_stmt *stmt;     
        sqlite3_prepare_v2(db, "insert into change_map (objectId, parent_objectId, object_name, physical_path, last_event, "
                               "timestamp, oper_complete, object_type, file_size, cr_index) values (?1, ?2, ?3, ?4, "
                               "?5, ?6, ?7, ?8, ?9, ?10);", -1, &stmt, NULL);       
        sqlite3_bind_text(stmt, 1, iter->objectId.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 2, iter->parent_objectId.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 3, iter->object_name.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 4, iter->physical_path.c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_text(stmt, 5, event_type_to_str(iter->last_event).c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_int(stmt, 6, iter->timestamp); 
        sqlite3_bind_int(stmt, 7, iter->oper_complete ? 1 : 0);
        sqlite3_bind_text(stmt, 8, object_type_to_str(iter->object_type).c_str(), -1, SQLITE_STATIC); 
        sqlite3_bind_int(stmt, 9, iter->file_size); 
        sqlite3_bind_int(stmt, 10, iter->cr_index); 

        rc = sqlite3_step(stmt); 
        if (SQLITE_DONE != rc) {
            LOG(LOG_ERR, "ERROR inserting data: %s", sqlite3_errmsg(db));
        }

        sqlite3_finalize(stmt);
    }

    sqlite3_close(db);

    return irods_filesystem_event_processor_error::SUCCESS;
}

static int query_callback_change_map(void *change_map_void_ptr, int argc, char** argv, char** columnNames) {

    if (nullptr == change_map_void_ptr) {
        LOG(LOG_ERR, "Invalid nullptr sent to change_map in %s", __FUNCTION__);
    }

    if (10 != argc) {
        LOG(LOG_ERR, "Invalid number of columns returned from change_map query in database.");
        return  irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    change_map_t *change_map = static_cast<change_map_t*>(change_map_void_ptr);

    change_descriptor entry{};
    entry.objectId = argv[0];
    entry.parent_objectId = argv[1];
    entry.object_name = argv[2];
    entry.object_type = str_to_object_type(argv[3]);
    entry.physical_path = argv[4]; 

    int oper_complete;
    int timestamp;
    int file_size;
    unsigned long long cr_index;

    try {
        oper_complete = boost::lexical_cast<int>(argv[5]);
        timestamp = boost::lexical_cast<time_t>(argv[6]);
        file_size = boost::lexical_cast<off_t>(argv[8]);
        cr_index = boost::lexical_cast<unsigned long long>(argv[9]);
    } catch( boost::bad_lexical_cast const& ) {
        LOG(LOG_ERR, "Could not convert the string to int returned from change_map query in database.");
        return  irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    entry.oper_complete = (oper_complete == 1);
    entry.timestamp = timestamp;
    entry.last_event = str_to_event_type(argv[7]);
    entry.file_size = file_size;
    entry.cr_index = cr_index;

    std::lock_guard<std::mutex> lock(change_table_mutex);
    change_map->insert(entry);

    return irods_filesystem_event_processor_error::SUCCESS;
}

static int query_callback_cr_index(void *cr_index_void_ptr, int argc, char** argv, char** columnNames) {

    if (nullptr == cr_index_void_ptr) {
        LOG(LOG_ERR, "Invalid nullptr sent to cr_index_ptr in %s", __FUNCTION__);
    }

    if (1 != argc) {
        LOG(LOG_ERR, "Invalid number of columns returned from cr_index query in database.");
        return  irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    unsigned long long *cr_index_ptr = static_cast<unsigned long long*>(cr_index_void_ptr);

    *cr_index_ptr = 0;

    if (nullptr != argv[0]) {
        try {
            *cr_index_ptr = boost::lexical_cast<unsigned long long>(argv[0]);
        } catch( boost::bad_lexical_cast const& ) {
            LOG(LOG_ERR, "Could not convert the string to int returned from change_map query in database.");
            return  irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
        }
    }

    return irods_filesystem_event_processor_error::SUCCESS;
}

int write_cr_index_to_sqlite(unsigned long long cr_index, const std::string& db_file) {

    sqlite3 *db;
    int rc;

    std::string serialize_file = db_file + ".db";
    rc = sqlite3_open(serialize_file.c_str(), &db);

    if (rc) {
        LOG(LOG_ERR, "Can't open %s for serialization.", serialize_file.c_str());
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }


    sqlite3_stmt *stmt;     
    sqlite3_prepare_v2(db, "insert into last_cr_index (cr_index) values (?1);", -1, &stmt, NULL);       
    sqlite3_bind_int(stmt, 1, cr_index); 

    rc = sqlite3_step(stmt); 

    if (SQLITE_DONE != rc && SQLITE_CONSTRAINT != rc) {
        LOG(LOG_ERR, "ERROR inserting data: %s", sqlite3_errmsg(db));
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return irods_filesystem_event_processor_error::SUCCESS;
}


int get_cr_index(unsigned long long& cr_index, const std::string& db_file) {

    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    std::string serialize_file = db_file + ".db";
    rc = sqlite3_open(serialize_file.c_str(), &db);

    if (rc) {
        LOG(LOG_ERR, "Can't open %s to read changemap index.", serialize_file.c_str());
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    rc = sqlite3_exec(db, "select max(cr_index) from last_cr_index", query_callback_cr_index, &cr_index, &zErrMsg);

    if (rc) {
        LOG(LOG_ERR, "Error querying change_map from db during de-serialization: %s", zErrMsg);
        sqlite3_close(db);
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    sqlite3_close(db);

    return irods_filesystem_event_processor_error::SUCCESS;
}


int deserialize_change_map_from_sqlite(change_map_t& change_map, const std::string& db_file) {

    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    std::string serialize_file = db_file + ".db";
    rc = sqlite3_open(serialize_file.c_str(), &db);

    if (rc) {
        LOG(LOG_ERR, "Can't open %s for de-serialization.", serialize_file.c_str());
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    rc = sqlite3_exec(db, "select objectId, parent_objectId, object_name, object_type, physical_path, oper_complete, "
                          "timestamp, last_event, file_size, cr_index from change_map", query_callback_change_map, &change_map, &zErrMsg);

    if (rc) {
        LOG(LOG_ERR, "Error querying change_map from db during de-serialization: %s", zErrMsg);
        sqlite3_close(db);
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    // delete contents of table using sqlite truncate optimizer
    rc = sqlite3_exec(db, "delete from change_map", NULL, NULL, &zErrMsg);
    
    if (rc) {
        LOG(LOG_ERR, "Error clearing out change_map from db during de-serialization: %s", zErrMsg);
        sqlite3_close(db);
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    sqlite3_close(db);

    return irods_filesystem_event_processor_error::SUCCESS;
}

int initiate_change_map_serialization_database(const std::string& db_file) {

    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;

    const char *create_table_str = "create table if not exists change_map ("
       "objectId char(256) primary key, "
       "cr_index integer, "
       "parent_objectId char(256), "
       "object_name char(256), "
       "physical_path char(256), "
       "last_event char(256), "
       "timestamp integer, "
       "oper_complete integer, "
       "object_type char(256), "
       "file_size integer)";

    // note:  storing cr_index as string because integer in sqlite is max of signed 64 bits
    const char *create_last_cr_index_table = "create table if not exists last_cr_index ("
       "cr_index integer primary key)";

    std::string serialize_file = db_file + ".db";
    rc = sqlite3_open(serialize_file.c_str(), &db);

    if (rc) {
        LOG(LOG_ERR, "Can't create or open %s.", serialize_file.c_str());
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    rc = sqlite3_exec(db, create_table_str,  NULL, NULL, &zErrMsg);
    
    if (rc) {
        LOG(LOG_ERR, "Error creating change_map table: %s", zErrMsg);
        sqlite3_close(db);
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }

    rc = sqlite3_exec(db, create_last_cr_index_table,  NULL, NULL, &zErrMsg);
    
    if (rc) {
        LOG(LOG_ERR, "Error creating last_cr_index table: %s", zErrMsg);
        sqlite3_close(db);
        return irods_filesystem_event_processor_error::SQLITE_DB_ERROR;
    }


    sqlite3_close(db);

    return irods_filesystem_event_processor_error::SUCCESS;
}

void add_entries_back_to_change_table(change_map_t& change_map, std::shared_ptr<change_map_t>& removed_entries) {

    std::lock_guard<std::mutex> lock(change_table_mutex);

    auto &change_map_seq = removed_entries->get<0>(); 
    for (auto iter = change_map_seq.begin(); iter != change_map_seq.end(); ++iter) {
        change_map.insert(*iter);
    }
}

