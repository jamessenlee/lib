#ifndef DAS_LIB_UTILS_H
#define DAS_LIB_UTILS_H

#include "input_object.h"
#include <limits.h>

namespace das_lib {

int get_and_check_uint32(const configio::DynamicRecord &record,
        int index, const std::string &name, uint32_t &value,
        uint32_t min_value = 0, uint32_t max_value = UINT_MAX,
        int vindex = -1, configio::IMessage *parent = NULL);
int get_and_check_uint64(const configio::DynamicRecord &record,
        int index, const std::string &name, unsigned long long &value,
        unsigned long long min_value = 0, unsigned long long max_value = ULLONG_MAX);
int get_and_check_int64(const configio::DynamicRecord &record,
        int index, const std::string &name, long long &value,
        long long min_value = 0, long long max_value = LLONG_MAX);

}//namespace

#endif
