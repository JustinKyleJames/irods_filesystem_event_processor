#ifndef IRODS_FILE_SYSTEM_CHANGE_TABLE_HPP
#define IRODS_FILE_SYSTEM_CHANGE_TABLE_HPP

#include "inout_structs.h"

#include "config.hpp"
#include <string>
#include <ctime>
#include <vector>
#include <set>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/filesystem.hpp>

// common headers
#include "../../common/irods_filesystem_event_processor_errors.hpp"
#include "../../common/change_table_avro.hpp"
#include "../../common/file_system_event_avro.hpp"


struct change_descriptor {
    unsigned long long            change_record_index;
    std::string                   object_identifier;
    std::string                   source_parent_object_identifier;     // BeeGFS provides the source_parent_object_identifier only
    std::string                   target_parent_object_identifier;     // Lustre provides the target_parent_object_identifier only
    std::string                   object_name;
    std::string                   source_physical_path; 
    std::string                   target_physical_path;     // the physical_path can be ascertained by the parent_fid and object_name
                                                            // however, if a parent is moved after calculating the physical_path, we 
                                                            // may have to look up the path using iRODS metadata
    file_system_event_aggregator::EventTypeEnum event_type; 
    time_t                        timestamp;
    bool                          oper_complete;
    file_system_event_aggregator::ObjectTypeEnum object_type;
    off_t                         file_size;
};

struct change_descriptor_seq_idx {};
struct change_descriptor_object_identifier_idx {};
struct change_descriptor_oper_complete_idx {};

typedef boost::multi_index::multi_index_container<
  change_descriptor,
  boost::multi_index::indexed_by<
    boost::multi_index::ordered_unique<
      boost::multi_index::tag<change_descriptor_seq_idx>,
      boost::multi_index::member<
        change_descriptor, unsigned long long, &change_descriptor::change_record_index
      >
    >,
    boost::multi_index::hashed_unique<
      boost::multi_index::tag<change_descriptor_object_identifier_idx>,
      boost::multi_index::member<
        change_descriptor, std::string, &change_descriptor::object_identifier
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


// This is only to faciliate writing the object_identifier to the root directory 
int write_object_identifier_to_root_dir(const std::string& root_path, const std::string& object_identifier, change_map_t& change_map);

int handle_close(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_mkdir(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_rmdir(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_unlink(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_rename(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_create(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_mtime(const fs_event::filesystem_event& event, change_map_t& change_map);
int handle_trunc(const fs_event::filesystem_event& event, change_map_t& change_map);

int remove_object_identifier_from_table(const std::string& object_identifier, change_map_t& change_map);

size_t get_change_table_size(change_map_t& change_map);

void print_change_table(const change_map_t& change_map);
bool entries_ready_to_process(change_map_t& change_map);
int serialize_change_map_to_sqlite(change_map_t& change_map, const std::string& db_file);
int deserialize_change_map_from_sqlite(change_map_t& change_map, const std::string& db_file);
int initiate_change_map_serialization_database(const std::string& db_file);
int write_change_table_to_avro_buf(const filesystem_event_aggregator_cfg_t *config_struct_ptr, boost::shared_ptr< std::vector<uint8_t>>& buffer,
                                          change_map_t& change_map, std::multiset<std::string>& current_active_object_identifier_list); 
void add_entries_back_to_change_table(change_map_t& change_map, std::shared_ptr<change_map_t>& removed_entries);
int add_avro_buffer_back_to_change_table(const boost::shared_ptr< std::vector<uint8_t>>& buffer, change_map_t& change_map, std::multiset<std::string>& current_active_object_identifier_list);
void remove_object_identifiers_in_avro_buffer_from_active_list(const boost::shared_ptr< std::vector<uint8_t>>& buffer, std::multiset<std::string>& current_active_object_identifier_list);
int get_change_record_index(unsigned long long& change_record_index, const std::string& db_file);
int write_change_record_index_to_sqlite(unsigned long long change_record_index, const std::string& db_file);


#endif


