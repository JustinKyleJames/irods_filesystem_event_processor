#ifndef IRODS_OPS_HPP
#define IRODS_OPS_HPP

#include "rodsType.h"
#include "inout_structs.h"
#include "config.hpp"
#include "rodsClient.h"

class irods_connection {
 public:
   unsigned int thread_number;
   rcComm_t *irods_conn;
   explicit irods_connection(unsigned int tnum) : thread_number(tnum), irods_conn(nullptr) {}
   ~irods_connection(); 
   int populate_irods_resc_id(filesystem_event_aggregator_cfg_t *config_struct_ptr);
   int instantiate_irods_connection(const filesystem_event_aggregator_cfg_t *config_struct_ptr, int thread_number);
};

#endif
