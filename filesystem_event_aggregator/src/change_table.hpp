#ifndef IRODS_FILE_SYSTEM_CHANGE_TABLE_HPP
#define IRODS_FILE_SYSTEM_CHANGE_TABLE_HPP

#include "inout_structs.h"

#include "config.hpp"
#include <string>
#include <ctime>
#include <vector>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/filesystem.hpp>

// common headers
#include "../../common/irods_filesystem_event_processor_errors.hpp"
#include "../../common/change_table_avro.hpp"


struct change_descriptor {
    unsigned long long            cr_index;
    std::string                   objectId;
    std::string                   parent_objectId;
    std::string                   object_name;
    std::string                   physical_path;     // the physical_path can be ascertained by the parent_fid and object_name
                                                     // however, if a parent is moved after calculating the physical_path, we 
                                                     // may have to look up the path using iRODS metadata
    file_system_event_aggregator::EventTypeEnum last_event; 
    time_t                        timestamp;
    bool                          oper_complete;
    file_system_event_aggregator::ObjectTypeEnum object_type;
    off_t                         file_size;
};

struct change_descriptor_seq_idx {};
struct change_descriptor_objectId_idx {};
struct change_descriptor_oper_complete_idx {};

typedef boost::multi_index::multi_index_container<
  change_descriptor,
  boost::multi_index::indexed_by<
    boost::multi_index::ordered_unique<
      boost::multi_index::tag<change_descriptor_seq_idx>,
      boost::multi_index::member<
        change_descriptor, unsigned long long, &change_descriptor::cr_index
      >
    >,
    boost::multi_index::hashed_unique<
      boost::multi_index::tag<change_descriptor_objectId_idx>,
      boost::multi_index::member<
        change_descriptor, std::string, &change_descriptor::objectId
      >
    >,
    boost::multi_index::hashed_non_unique<
      boost::multi_index::tag<change_descriptor_oper_complete_idx>,
      boost::multi_index::member<
        change_descriptor, bool, &change_descriptor::oper_complete
      >
    >

  >
> change_map_t;


// This is only to faciliate writing the objectId to the root directory 
int write_objectId_to_root_dir(const std::string& root_path, const std::string& objectId, change_map_t& change_map);

int handle_close(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);
int handle_mkdir(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);
int handle_rmdir(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);
int handle_unlink(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);
int handle_rename(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, const std::string& old_physical_path, 
                     change_map_t& change_map);
int handle_create(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);
int handle_mtime(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);
int handle_trunc(unsigned long long cr_index, const std::string& fs_mount_path, const std::string& objectId, const std::string& parent_objectId,
                     const std::string& object_name, const std::string& physical_path, change_map_t& change_map);


int remove_objectId_from_table(const std::string& objectId, change_map_t& change_map);

size_t get_change_table_size(change_map_t& change_map);

void print_change_table(const change_map_t& change_map);
bool entries_ready_to_process(change_map_t& change_map);
int serialize_change_map_to_sqlite(change_map_t& change_map, const std::string& db_file);
int deserialize_change_map_from_sqlite(change_map_t& change_map, const std::string& db_file);
int initiate_change_map_serialization_database(const std::string& db_file);
int write_change_table_to_avro_buf(const filesystem_event_aggregator_cfg_t *config_struct_ptr, boost::shared_ptr< std::vector<uint8_t>>& buffer,
                                          change_map_t& change_map, std::set<std::string>& current_active_objectId_list); 
void add_entries_back_to_change_table(change_map_t& change_map, std::shared_ptr<change_map_t>& removed_entries);
int add_avro_buffer_back_to_change_table(const boost::shared_ptr< std::vector<uint8_t>>& buffer, change_map_t& change_map, std::set<std::string>& current_active_objectId_list);
void remove_objectId_from_active_list(const boost::shared_ptr< std::vector<uint8_t>>& buffer, std::set<std::string>& current_active_objectId_list);
int get_cr_index(unsigned long long& cr_index, const std::string& db_file);
int write_cr_index_to_sqlite(unsigned long long cr_index, const std::string& db_file);


#endif


