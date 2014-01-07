#ifndef DAS_LIB_LOG_H
#define	DAS_LIB_LOG_H

#include <com_log.h>

#define PBRPC_LOG(_level_, _fmt_, args...)                              \
    do {                                                                \
        com_writelog(                                                   \
            _level_, "[%s:%d|%s] "_fmt_,                                \
            __FILE__, __LINE__, __FUNCTION__, ##args);                  \
    } while (0);

#define DL_LOG_FATAL(_fmt_, args...) PBRPC_LOG(COMLOG_FATAL, _fmt_, ##args)
#define DL_LOG_WARNING(_fmt_, args...) PBRPC_LOG(COMLOG_WARNING, _fmt_, ##args)
#define DL_LOG_TRACE(_fmt_, args...) PBRPC_LOG(COMLOG_TRACE, _fmt_, ##args)

#endif

