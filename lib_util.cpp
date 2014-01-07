#include "lib_util.h"
#include <limits.h>

namespace das_lib {

int get_and_check_uint32(const configio::DynamicRecord &record,
        int index, const std::string &name, uint32_t &value,
        uint32_t min_value, uint32_t max_value,
        int vindex, configio::IMessage *parent)
{
    if (0 != record.get_uint32(index, value, vindex, parent)) {
        WARNING_LOG("fail to get [%s] from DynamicRecord", name.c_str());
        return -1;
    }
    if (value < min_value || value > max_value) {
        WARNING_LOG("value of [%s] is [%u], not in scope [%u, %u]",
                name.c_str(), value, min_value, max_value);
        return -1;
    }

    return 0;
}

int get_and_check_uint64(const configio::DynamicRecord &record,
        int index, const std::string &name, unsigned long long &value,
        unsigned long long min_value, unsigned long long max_value)
{
    if (0 != record.get_uint64(index, value)) {
        WARNING_LOG("fail to get [%s] from DynamicRecord", name.c_str());
        return -1;
    }
    if (value < min_value || value > max_value) {
        WARNING_LOG("value of [%s] is [%llu], not in scope [%llu, %llu]",
                name.c_str(), value, min_value, max_value);
        return -1;
    }

    return 0;
}

int get_and_check_int64(const configio::DynamicRecord &record,
        int index, const std::string &name, long long &value,
        long long min_value, long long max_value)
{
    if (0 != record.get_int64(index, value)) {
        WARNING_LOG("fail to get [%s] from DynamicRecord", name.c_str());
        return -1;
    }
    if (value < min_value || value > max_value) {
        WARNING_LOG("value of [%s] is [%lld], not in scope [%lld, %lld]",
                name.c_str(), value, min_value, max_value);
        return -1;
    }

    return 0;
}

}//namespace
