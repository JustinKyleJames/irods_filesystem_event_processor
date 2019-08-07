#ifndef IRODS_BEEGFS_INOUT_STRUCTS
#define IRODS_BEEGFS_INOUT_STRUCTS

typedef struct {
    int buflen;
    unsigned char *buf;
} irodsBeegfsApiInp_t;
#define IrodsBeegfsApiInp_PI "int buflen; bin *buf(buflen);"

typedef struct {
    int status;
} irodsBeegfsApiOut_t;
#define IrodsBeegfsApiOut_PI "int status;"

#endif


