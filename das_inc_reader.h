#ifndef DAS_INC_READER_H
#define DAS_INC_READER_H

#include <vector>
#include <map>
#include <dirent.h>
#include "configio.h"

namespace das_lib {
struct das_inc_conf_t {
    std::string configio_xml_path;
    unsigned long long last_event_id;
    unsigned long long max_lines_per_round;
    std::string pipe_name;
};

struct topic_info_t {
    std::string cur_file;
    unsigned long long cur_line;
};
 
class DASIncReader {
public:
    DASIncReader();

    ~DASIncReader();

    int init(const das_inc_conf_t &conf);

    int read_next(configio::DynamicRecord &record);

    int close();

    enum read_result_t {
        READ_FAIL = -1,
        READ_SUCC = 0,
        READ_END = 1
    };

    bool is_end_of_reader();

    void get_topic_info(topic_info_t& info) const;
    
private:

    configio::InputObject _reader;

    unsigned long long _start_event_ids;
    bool _has_sought;  // 是否已经成功seek到了正确的位置
    unsigned long long _last_event_ids;

    int _max_lines_per_round;

    // 当前的reader已经读取的行数
    int _cur_lines;

    std::string _desc;
};

}//das lib


#endif // DAS_INC_READER_H
