#ifndef IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING
#define IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING

#include <ctime>
#include <iomanip>

#define LOG_FATAL    (1)
#define LOG_ERR      (2)
#define LOG_WARN     (3)
#define LOG_INFO     (4)
#define LOG_DBG      (5)
#define LOG_TRACE    (6)

#define LOG(level, format_str, ...) do {  \
                                if (level <= log_level) { \
                                    auto IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_t = std::time(nullptr); \
                                    auto IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_tm = *std::localtime(&IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_t); \
                                    std::ostringstream IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_oss; \
                                    IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_oss << std::put_time(&IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_tm, "%d-%m-%Y %H-%M-%S"); \
                                    auto IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str = IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_oss.str(); \
                                    switch (level) { \
                                      case 1: \
                                        fprintf(dbgstream, (IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str + std::string(":FATAL:%s: ") + format_str + "\n").c_str(), thread_identifier, ##__VA_ARGS__); \
                                        fflush(dbgstream); \
                                        break; \
                                      case 2: \
                                        fprintf(dbgstream, (IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str + std::string(":ERROR:%s: ") + format_str + "\n").c_str(), thread_identifier, ##__VA_ARGS__); \
                                        fflush(dbgstream); \
                                        break; \
                                      case 3: \
                                        fprintf(dbgstream, (IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str + std::string(": WARN:%s: ") + format_str + "\n").c_str(), thread_identifier, ##__VA_ARGS__); \
                                        fflush(dbgstream); \
                                        break; \
                                      case 4: \
                                        fprintf(dbgstream, (IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str + std::string(": INFO:%s: ") + format_str + "\n").c_str(), thread_identifier, ##__VA_ARGS__); \
                                        fflush(dbgstream); \
                                        break; \
                                      case 5: \
                                        fprintf(dbgstream, (IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str + std::string(":DEBUG:%s: ") + format_str + "\n").c_str(), thread_identifier, ##__VA_ARGS__); \
                                        fflush(dbgstream); \
                                        break; \
                                      case 6: \
                                        fprintf(dbgstream, (IRODS_EVENT_AGGREGATOR_DEBUG_LOGGING_time_str + std::string(":TRACE:%s: ") + format_str + "\n").c_str(), thread_identifier, ##__VA_ARGS__); \
                                        fflush(dbgstream); \
                                        break; \
                                    } \
                                } \
                            } while (0)
extern FILE *dbgstream;
extern int  log_level;
extern thread_local char *thread_identifier;

#endif
