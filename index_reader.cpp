/**
 * @file bd_index_reader.cpp
 * @brief index reader for budget and das 
 * @author lihaibing01@baidu.com
 * @date 2013-03-07
 */

#include "index_reader.h"

namespace nova {
namespace bs {

const int IndexReader::read_next(configio::DynamicRecord &record) {

    while (_reader_it != _reader.end()) {
        int ret = _reader_it->second.read_next(record);
        switch (ret) {
        case -1: 
            FATAL_LOG("fail to read next das inc");
            _reader_it = _reader.begin();
            return READ_FAIL; 
        case 0:
            return READ_SUCC;
        case 1:
            ++_reader_it;
            continue;
        default:
            FATAL_LOG("fail to read next das inc");
            _reader_it = _reader.begin();
            return READ_FAIL; 
        }
    }
    _reader_it = _reader.begin();
    return READ_END;
}

const bool IndexReader::is_reach_end() {
    _IndexReader::iterator reader_it = _reader.begin();
    while (reader_it != _reader.end()) {
        if (!reader_it->second.is_end_of_reader()) {
            return false;
        }
        ++reader_it;
    }
    return true;
}


const int IndexReader::log_index_info() {
    _InfoIndex index_info = get_index_info();
    _InfoIndex::iterator info_it = index_info.begin();

    for (; info_it != index_info.end(); ++info_it) {
        std::vector<topic_reader_info_t>::iterator reader_it = info_it->reader_infos.begin();
        for (; reader_it != info_it->reader_infos.end(); ++reader_it) {
            WARNING_LOG("topic_name:%s, current file:%s,current line:%llu", 
                info_it->topic_name.c_str(), reader_it->cur_file.c_str(), reader_it->cur_line);
        }
    }
    return 0;
}

const IndexReader::_InfoIndex IndexReader::get_index_info() {
    _InfoIndex index_info;
    _IndexReader::iterator reader_it = _reader.begin();
    
    index_info.reserve(_reader.size());
    while (reader_it != _reader.end()) {
        topic_info_t topic_info = reader_it->second.get_topic_info();
        index_info.push_back(topic_info);
        ++reader_it;
    }
    return index_info;
}

} // bs
}//nova

