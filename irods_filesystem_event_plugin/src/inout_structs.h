#ifndef IRODS_FS_EVENT_INOUT_STRUCTS
#define IRODS_FS_EVENT_INOUT_STRUCTS

typedef struct {
    int buflen;
    unsigned char *buf;
} irodsFsEventApiInp_t;
#define IrodsFsEventApiInp_PI "int buflen; bin *buf(buflen);"

typedef struct {
    int status;
} irodsFsEventApiOut_t;
#define IrodsFsEventApiOut_PI "int status;"

#endif


