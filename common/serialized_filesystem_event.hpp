#ifndef SERIALIZED_FILESYSTEM_EVENT_HPP
#define SERIALIZED_FILESYSTEM_EVENT_HPP

typedef struct serialized_filesystem_event {
    size_t index;
    char event_type[10];
    char root_path[4096];                              
    char entryId[4096 + sizeof (u_int32_t)];        
    char targetParentId[4096 + sizeof (u_int32_t)];   
    char basename[4096];
    char full_target_path[4096]; 
    char full_path[4096];
} serialized_filesystem_event_t;

void create_event(const size_t index, 
        const std::string event_type, 
        const std::string root_path, 
        const std::string entryId,
        const std::string targetParentId, 
        const std::string basename, 
        const std::string full_target_path, 
        const std::string full_path,
        serialized_filesystem_event& event) {

    event.index = index;

    strncpy(event.event_type, event_type.c_str(), sizeof(event.event_type) - 1);
    event.event_type[sizeof(event.event_type) - 1] = 0;

    strncpy(event.root_path, root_path.c_str(), sizeof(event.root_path) - 1);
    event.root_path[sizeof(event.root_path) - 1] = 0;

    strncpy(event.entryId, entryId.c_str(), sizeof(event.entryId) - 1);
    event.entryId[sizeof(event.entryId) - 1] = 0;

    strncpy(event.targetParentId, targetParentId.c_str(), sizeof(event.targetParentId) - 1);
    event.targetParentId[sizeof(event.targetParentId) - 1] = 0;

    strncpy(event.basename, basename.c_str(), sizeof(event.basename) - 1);
    event.basename[sizeof(event.basename) - 1] = 0;

    strncpy(event.full_target_path, full_target_path.c_str(), sizeof(event.full_target_path) - 1);
    event.full_target_path[sizeof(event.full_target_path) - 1] = 0;

    strncpy(event.full_path, full_path.c_str(), sizeof(event.full_path) - 1);
    event.full_path[sizeof(event.full_path) - 1] = 0;
}

#endif

