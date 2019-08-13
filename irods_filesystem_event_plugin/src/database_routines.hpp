#ifndef IRODS_FS_EVENT_API_DB_ROUTINES
#define IRODS_FS_EVENT_API_DB_ROUTINES

#include "icatStructs.hpp"

int cmlGetNSeqVals( icatSessionStruct *icss, size_t n, std::vector<rodsLong_t>& sequences );

#if MY_ICAT
void setMysqlIsolationLevelReadCommitted(icatSessionStruct *icss); 
#endif


#endif

