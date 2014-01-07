// Copyright (c) 2010, 2011 Baidu.com, Inc. All Rights Reserved
//
// Author: wangqiushi@baidu.com

#ifndef NOVA_BS_DAS_INC_READER_H
#define NOVA_BS_DAS_INC_READER_H

#include <vector>
#include <map>
#include <dirent.h>
#include "configio.h"

namespace das_lib {
struct normal_inc_conf_t {
    std::string configio_xml_path;
    unsigned long long last_event_id;
    unsigned long long max_lines_per_round;
    std::string pipe_name;
};
 
    
struct das_inc_conf_t {
public:
    std::string inc_dir;
    std::string inc_fname;
    std::string pipe_name; //流的名称
    std::string configio_xml_path;
    unsigned long long last_event_id;
    bool is_load_nebula;
    int max_lines_per_round;

    std::vector<int> pipelets;

    das_inc_conf_t()
        : last_event_id(0)
        , is_load_nebula(false)
        , max_lines_per_round(-1)
    {
    }

    int init_pipelets();
};

struct das_base_conf_t {
public:
    std::string configio_xml_path;
    std::string base_dir;
    unsigned long long last_event_id;
    
    das_base_conf_t()
        : last_event_id(0)
    {
    }
};

struct topic_reader_info_t {
    std::string cur_file;
    unsigned long long cur_line;
};
struct topic_info_t {
    std::string topic_name;
    std::vector<topic_reader_info_t> reader_infos; 
};

class DASIncReader {
public:
    DASIncReader();

    ~DASIncReader();

    int init(const normal_inc_conf_t &conf);
    int init(const das_inc_conf_t &conf);
    int init(const das_base_conf_t &conf);

    int read_next(configio::DynamicRecord &record);

    int close();

    enum read_result_t {
        READ_SUCC = 0,
        READ_END = 1
    };

    //每一个pipelet都读完才认为读完    
    bool is_end_of_reader();
    
    const topic_info_t get_topic_info() const;
private:
    int check_matching_files();

    typedef std::map<int, configio::InputObject*> _ReadersType;

    // one reader for each pipelet.
    _ReadersType _readers;

    _ReadersType::iterator _rit;

    std::map<uint32_t, unsigned long long> _start_event_ids;
    std::map<uint32_t, bool> _has_sought;  // 是否已经成功seek到了正确的位置
    std::map<uint32_t, unsigned long long> _last_event_ids;

    // 在一轮追增量过程中，每个reader最多读取的增量数量
    // init之后不可改变
    int _max_lines_per_round;

    // 当前的reader已经读取的行数
    int _cur_lines;

    std::string _desc;
};

}//das lib


#endif // _DAS_INC_READER_H_
