/**
 * @file bd_index_reader.h
 * @brief index reader for budget and das 
 * @author lihaibing01@baidu.com
 * @date 2013-03-07
 */

#pragma once
#ifndef NOVA_BS_INDEX_READER_H
#define NOVA_BS_INDEX_READER_H

#include "das_inc_reader.h"

namespace das_lib {
class IndexReader {
public:
    IndexReader(){}
    ~IndexReader(){}
   
    enum read_result_t {
        READ_FAIL = -1,
        READ_SUCC = 0,
        READ_END = 1
    };

    template <class Conf_t>
    const int subscribe_topic(std::string topic_name, Conf_t conf) {
        int ret = _reader[topic_name].init(conf);
        if (0 != ret) {
            printf("fail to init das_inc_reader");
            return -1;
        }
        _reader_it = _reader.begin();
        return ret;
    }

    const int read_next(configio::DynamicRecord &record);
    const bool is_reach_end();
    const int log_index_info();

private:
    typedef std::vector<topic_info_t> _InfoIndex;
    typedef std::map<std::string, DASIncReader> _IndexReader;

    const _InfoIndex get_index_info();

    _IndexReader _reader;
    _IndexReader::iterator _reader_it;
};

}//nova

#endif
