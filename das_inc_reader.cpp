// Copyright (c) 2010, 2011 Baidu.com, Inc. All Rights Reserved
//
// Author: wangqiushi@baidu.com

#include "das_inc_reader.h"
#include "cm_utility/string_utils.h"

namespace nova {
namespace bs {
DASIncReader::DASIncReader()
    : _max_lines_per_round(-1)
    , _cur_lines(0)
{
}

DASIncReader::~DASIncReader()
{
    close();
}

int DASIncReader::init(const normal_inc_conf_t &conf)
{
    int ret = 0;

    _max_lines_per_round = conf.max_lines_per_round;
    _cur_lines = 0;
    _desc = conf.pipe_name;
    
    // readers.
    _readers.clear();
    _start_event_ids.clear();
    _has_sought.clear();
    _last_event_ids.clear();
    
    configio::InputObject *reader = new (std::nothrow) configio::InputObject();
    if (NULL == reader) {
        FATAL_LOG("fail to create InputObject, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    ret = reader->init(conf.configio_xml_path.c_str());
    if (0 != ret) {
        FATAL_LOG("fail to init InputObject, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }
    // open and seek.
    ret = reader->open();
    if (0 != ret) {
        FATAL_LOG("fail to open InputObject, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    ret = reader->seek(&conf.last_event_id, sizeof(conf.last_event_id));
    if (ret < 0) {
        FATAL_LOG("fail to seek to event_id[%llu], conf[%s], ret[%d].",
                conf.last_event_id, conf.configio_xml_path.c_str(),ret);
        return -1;
    }
    _start_event_ids[0] = conf.last_event_id;
    _has_sought[0] = (0 == ret);

    _readers[0] = reader;
    _last_event_ids[0] = 0;

    _rit = _readers.begin();

    // check if there is any matching files in reader dir.
    if (0 != check_matching_files()) {
        FATAL_LOG("fail to get matching files, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    return 0;
}

int DASIncReader::init(const das_base_conf_t &conf)
{
    int ret = 0;

    // reader.
    _readers.clear();
    _start_event_ids.clear();
    _has_sought.clear();
    _last_event_ids.clear();

    configio::InputObject *reader = new (std::nothrow) configio::InputObject();
    if (NULL == reader) {
        FATAL_LOG("fail to create InputObject, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    ret = reader->init(conf.configio_xml_path.c_str());
    if (0 != ret) {
        FATAL_LOG("fail to init InputObject, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    // reset file dir.
    // 基准文件夹有0/1切换，因此不能写在xml中
    std::string dir(conf.base_dir.c_str());
    ret = reader->set_meta_info(
            configio::READER_LITERAL_FILE_LINE_CHILD_SRC_FILE_DIR,
            (void*)&dir);
    if (0 != ret) {
        FATAL_LOG("fail to set meta[%s] of InputObject, conf[%s].",
                configio::READER_LITERAL_FILE_LINE_CHILD_SRC_FILE_DIR,
                conf.configio_xml_path.c_str());
        return -1;
    }
                
    // open.    
    ret = reader->open();
    if (0 != ret) {
        FATAL_LOG("fail to open InputObject, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    _readers[0] = reader;
    _start_event_ids[0] = 0;
    _has_sought[0] = true;
    _last_event_ids[0] = 0;
    
    // iterator.
    _rit = _readers.begin();
    
    // check if there is any matching files in reader dir.
    if (0 != check_matching_files()) {
        FATAL_LOG("fail to get matching files, conf[%s].", dir.c_str());
        return -1;
    }

    return 0;
}

int DASIncReader::init(const das_inc_conf_t &conf)
{
    int ret = 0;

    _max_lines_per_round = conf.max_lines_per_round;
    _cur_lines = 0;
    _desc = conf.pipe_name;
    
    // readers.
    _readers.clear();
    _start_event_ids.clear();
    _has_sought.clear();
    _last_event_ids.clear();
    
    WARNING_LOG("init DASIncReader, pipelets[%zu], xml[%s]",
            conf.pipelets.size(), conf.configio_xml_path.c_str());
    
    for (std::vector<int>::const_iterator it = conf.pipelets.begin();
        it != conf.pipelets.end(); ++it) {
        
        configio::InputObject *reader = new (std::nothrow) configio::InputObject();
        if (NULL == reader) {
            FATAL_LOG("fail to create InputObject, conf[%s], pipelet[%d].",
                    conf.configio_xml_path.c_str(), *it);
            return -1;
        }

        ret = reader->init(conf.configio_xml_path.c_str());
        if (0 != ret) {
            FATAL_LOG("fail to init InputObject, conf[%s], pipelet[%d].",
                    conf.configio_xml_path.c_str(), *it);
            return -1;
        }

        // reset file dir and file name for each InpubObject.
        // dir: _conf.inc_dir + "/" + pipelet_id
        // fname: _conf.inc_fname;
        std::string inc_dir;
        cm_utility::string_appendf(&inc_dir,"%s/%d/", conf.inc_dir.c_str(), *it);
        std::string str_inc_dir(inc_dir.c_str());
        
        ret = reader->set_meta_info(
                configio::READER_LITERAL_INC_FILE_CHILD_SRC_FILE_DIR,
                (void*)&str_inc_dir);
        if (0 != ret) {
            FATAL_LOG("fail to set meta[%s] of InputObject, conf[%s], pipelet[%d].",
                    configio::READER_LITERAL_INC_FILE_CHILD_SRC_FILE_DIR,
                    conf.configio_xml_path.c_str(),
                    *it);
            return -1;
        }
        
        std::string str_inc_fname(conf.inc_fname.c_str());
        ret = reader->set_meta_info(
                configio::READER_LITERAL_INC_FILE_CHILD_SRC_FILE_PREFIX,
                (void*)&str_inc_fname);
        if (0 != ret) {
            FATAL_LOG("fail to set meta[%s] of InputObject, conf[%s], pipelet[%d].",
                    configio::READER_LITERAL_INC_FILE_CHILD_SRC_FILE_PREFIX,
                    conf.configio_xml_path.c_str(),
                    *it);
            return -1;
        }

        WARNING_LOG("init reader, pipelet[%d], dir[%s], file prefix[%s],"
                " event_id[%llu], xml[%s].",
                *it, str_inc_dir.c_str(), str_inc_fname.c_str(),
                conf.last_event_id, conf.configio_xml_path.c_str());

        // open and seek.
        ret = reader->open();
        if (0 != ret) {
            FATAL_LOG("fail to open , conf[%s], pipelet[%d].",
                    conf.configio_xml_path.c_str(), *it);
            return -1;
        }

        ret = reader->seek(&conf.last_event_id, sizeof(conf.last_event_id));
        if (ret < 0) {
            FATAL_LOG("fail to seek to event_id[%llu], conf[%s], pipelet[%d], ret[%d].",
                    conf.last_event_id, conf.configio_xml_path.c_str(), *it, ret);
            return -1;
        }
        WARNING_LOG("seek to event_id[%llu], ret[%d].", conf.last_event_id, ret);
        _start_event_ids[*it] = conf.last_event_id;
        _has_sought[*it] = (0 == ret);

        _readers[*it] = reader;
        _last_event_ids[*it] = 0;
    }

    _rit = _readers.begin();
    
    // check if there is any matching files in reader dir.
    if (0 != check_matching_files()) {
        FATAL_LOG("fail to get matching files, conf[%s].",
                conf.configio_xml_path.c_str());
        return -1;
    }

    return 0;
}
    
int DASIncReader::check_matching_files()
{
    for (_ReadersType::iterator it = _readers.begin();
            it != _readers.end(); ++it) {
        std::string dir;
        std::vector<std::string> fnames;

        // dir.
        if (0 != it->second->get_meta_info(configio::READER_META_DIR,
                                            (void*)&dir)) {
            FATAL_LOG("fail to get meta info[%s], pipelet[%d].",
                    configio::READER_META_DIR, it->first);
        }
        
        // all matching file names.
        if (0 != it->second->get_meta_info(configio::READER_META_FILE_NAMES,
                                            (void*)&fnames)) {
            FATAL_LOG("fail to get meta info[%s], dir[%s], pipelet[%d].",
                    configio::READER_META_FILE_NAMES, dir.c_str(), it->first);
        }
        if (fnames.empty()) {
            FATAL_LOG("fail to match any files, dir[%s], pipelet[%d].",
                    dir.c_str(), it->first);
            return -1;
        }
    }

    return 0;
}

bool DASIncReader::is_end_of_reader()
{
    _ReadersType::iterator it = _readers.begin();
    for(; it != _readers.end(); ++it) {
        bool reach_end = false;
        if (_has_sought[it->first]) {
            // 如果这个pipelet的数据已经seek成功，到reader中查找是否读取到文件结尾
            it->second->get_meta_info(configio::READER_META_REACH_END, &reach_end);
        } else {
            // 如果这个pipelet的数据还没有seek成功，认为是到达文件结尾
            reach_end = true;
        }

        if(!reach_end) {
            return false;
        }
    }
    
    return true;
}

int DASIncReader::read_next(configio::DynamicRecord &record)
{
    int ret = 0;
    unsigned long long cur_event_id;

    while (_rit != _readers.end()) {
        // check lines.
        if (_max_lines_per_round > 0 &&
            _cur_lines == _max_lines_per_round) {
            // 读取了足够的行数，切换到下一个reader
            WARNING_LOG("read enough lines[%d], max lines[%d], reader[%d]",
                    _cur_lines, _max_lines_per_round, _rit->first);
            ++_rit;
            _cur_lines = 0;
            continue;
        }
       
        // 如果init的时候文件还不存在导致seek返回1，这里需要重新seek到正确的位置
        // 
        if (!_has_sought[_rit->first]) {
            unsigned long long start_event_id = _start_event_ids[_rit->first];
            ret = _rit->second->seek(&start_event_id, sizeof(start_event_id));
            
            if(ret != 0 && ret != 1) {
                FATAL_LOG("fail to seek to event_id[%llu], pipelet[%d], ret[%d].",
                        start_event_id, _rit->first, ret);
                return -1;
            }

            if (0 == ret) {
                _has_sought[_rit->first] = true;
            } else if (1 == ret) {
                // seek到了文件结尾，仍然没有找到
                // do nothing，后面的代码会跳过这个reader
            }
        }
        
        // read next record.
        if (_has_sought[_rit->first]) {
            ret = _rit->second->get_next_record(record);
        } else {
            ret = 1;
        }

        if (0 == ret) {
            // check event id.
            ret = record.get_uint64(1, cur_event_id);
            if (0 != ret) {
                FATAL_LOG("fail to get event id from record");
                return -1;
            }

            if (cur_event_id < _last_event_ids[_rit->first]) {
                FATAL_LOG("current_event_id[%llu] is less than previous[%llu]",
                        cur_event_id,
                        _last_event_ids[_rit->first]);
                return -1;
            }
            
            _last_event_ids[_rit->first] = cur_event_id;
            _cur_lines++;
           
            return 0;
        } else if (1 == ret) {
            ++_rit;
            _cur_lines = 0;
            continue;
        } else {
            FATAL_LOG("fail to get next record.");
            continue;
        }
    } // end while


    _rit = _readers.begin();
    _cur_lines = 0;

    return 1;
}

int DASIncReader::close()
{
    for (_rit = _readers.begin(); _rit != _readers.end(); ++_rit) {
        _rit->second->close();
        delete _rit->second;
        _rit->second = NULL;
    }
    _readers.clear();
    
    _rit = NULL;

    return 0;
}

const topic_info_t DASIncReader::get_topic_info() const {
    topic_info_t topic_info;
    topic_reader_info_t reader_info;
    _ReadersType::const_iterator reader_it = _readers.begin();

    topic_info.topic_name = _desc;
    std::string cur_file_name;
    while (reader_it != _readers.end()) {
        reader_it->second->get_meta_info(
                configio::READER_META_CUR_FULL_FILE_NAME, (void*)&cur_file_name);
        reader_it->second->get_meta_info(
                configio::READER_META_CUR_LINE_NO, (void*)&reader_info.cur_line);
        reader_info.cur_file = cur_file_name.c_str();
        topic_info.reader_infos.push_back(reader_info);
        ++reader_it;
    }
    return topic_info;
}


int das_inc_conf_t::init_pipelets()
{
    // pipelets.
    DIR *dir = NULL;
    struct dirent *dirt = NULL;
    struct stat st;
     
    pipelets.clear();

    dir = opendir(inc_dir.c_str());
    if (NULL == dir) {
        FATAL_LOG("fail to open dir[%s]", inc_dir.c_str());
        return -1;
    }
    
    while ((dirt = readdir(dir)) != NULL) {
        if ('.' == dirt->d_name[0]) {
            continue;
        }

        std::string cur_fname;
        cm_utility::string_appendf(&cur_fname,"%s/%s", inc_dir.c_str(), dirt->d_name);

        if (0 != stat(cur_fname.c_str(), &st)) {
            FATAL_LOG("fail to get stat of file[%s]", cur_fname.c_str());
            return -1;
        }

        if (S_ISDIR(st.st_mode)) {
            int pipelet = atoi(dirt->d_name);
            pipelets.push_back(pipelet);
        }
    }

    closedir(dir);
    dir = NULL;
    
    return 0;
}

}//bs
}//nova
