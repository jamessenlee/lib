#ifndef DAS_LIB_UTILS_H
#define DAS_LIB_UTILS_H

#include "input_object.h"
#include <limits.h>

namespace das_lib {

//@param:   _partition_num 分库总数
//@param:   _partition_idx 本机BS所属分库编号, 0为起始号
class PartitionArg {
public:
    PartitionArg() : _partition_idx(0), _partition_num(1) {}
 
    PartitionArg(uint32_t part_idx, uint32_t part_num) :
        _partition_idx(part_idx),
        _partition_num(part_num) {}

    PartitionArg& set_partition_num(uint32_t part_num) {
        _partition_num = part_num;
        return *this;
    }

    PartitionArg& set_partition_idx(uint32_t part_idx) {
        _partition_idx = part_idx;
        return *this;
    }
     
    bool match_partition_unit(u_int unit_id) const {
        return (_partition_num <= 1 || unit_id % _partition_num == _partition_idx);
    }

    uint32_t get_partition_idx() const {
        return _partition_idx;
    }

    uint32_t get_partition_num() const {
        return _partition_num;

    }
    
private:

    uint32_t _partition_idx;
    
    uint32_t _partition_num;
    
};

// a set of method to cast from different types
// from protobuf code
template<typename To, typename From>
inline To implicit_cast(From const &f) {
    return f;
}
                                                 
template<typename To, typename From>     // use like this: down_cast<T*>(foo);
inline To down_cast(From* f) {                   // so we only accept pointers
     
     
    // Ensures that To is a sub-type of From *.  This test is here only
    // optimized build at run-time, as it will be optimized away
    // completely.
    if (false) {
        implicit_cast<From*, To>(0);
    }

#if !defined(NDEBUG)
    assert(f == NULL || dynamic_cast<To>(f) != NULL);  // RTTI: debug mode only!
#endif
    return static_cast<To>(f);
 }

inline unsigned long never_zero(unsigned long x)
{
    return 0 != x ? x : 1;
}

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
