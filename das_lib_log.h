#ifndef  DAS_LIB_LOG_H
#define  DAS_LIB_LOG_H

#include "com_log.h"

#define DL_WRITE_LOG(_loglevel_, _fmt_, args...) \
    do { \
        com_writelog(_loglevel_, "[%s:%s:%d] "_fmt_, __FILE__, __FUNCTION__, __LINE__, ##args); \
    } while (0);

#define DL_LOG_FATAL(_fmt_, args...) \
    do { \
        DL_WRITE_LOG(COMLOG_FATAL, _fmt_, ##args); \
    } while (0);

#define DL_LOG_WARNING(_fmt_, args...) \
    do { \
        DL_WRITE_LOG(COMLOG_WARNING, _fmt_, ##args); \
    } while (0);

#define DL_LOG_NOTICE(_fmt_, args...) \
    do { \
        com_writelog(COMLOG_NOTICE, _fmt_, ##args); \
    } while (0);

#define DL_LOG_DEBUG(_fmt_, args...) \
    do { \
        if (com_log_enabled(COMLOG_DEBUG) == 1) { \
            DL_WRITE_LOG(COMLOG_DEBUG, _fmt_, ##args); \
        } \
    } while (0);

#define DL_LOG_TRACE(_fmt_, args...) \
    do { \
        DL_WRITE_LOG(COMLOG_TRACE, _fmt_, ##args); \
    } while (0);


#endif  //DAS_LIB_LOG_H

