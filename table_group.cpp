#include "table_group.h"
#include "table_manager.hpp"
#include "das_lib_log.h"

namespace das_lib {

TableGroup::TableGroup()
{

};

bool TableGroup::register_table_info(const table_info_t& table_info)
{
    //1.首先加入到管理顺序容器里面
    //2.再按照不同的层级加入到不同的增量处理顺序容器中
    TableRegisteryType::iterator iter = 
        std::find(_table_manager_list.begin(),_table_manager_list.end(),
            table_info);

    if (iter != _table_manager_list.end()) {
        DL_LOG_FATAL("Table[%s] is already registered",table_info.p_table_mgr->desc().c_str());
        return false;
    }
    
    _table_manager_list.push_back(table_info);

    TableRegisteryType& inc_table_group = _inc_schedule_info[table_info.inc_level]; 
    inc_table_group.push_back(table_info);
    _table_info_map[table_info.table_name] = table_info;
 
    return true;
}

TableGroup::TableGroup(const TableGroup& rhs)
{
    DL_LOG_TRACE("in copy constructor");
    TableRegisteryType::const_iterator iter = 
        rhs._table_manager_list.begin();

    for (;iter != rhs._table_manager_list.end(); ++ iter) {
        table_info_t table_info;
        table_info.p_table_mgr = iter->p_table_mgr->clone(this);
        table_info.table_name = iter->p_table_mgr->desc();
        table_info.inc_level = iter->inc_level;

        register_table_info(table_info);
    }
}

TableGroup::~TableGroup()
{
    TableRegisteryType::iterator iter = 
        _table_manager_list.begin();

    for (; iter != _table_manager_list.end(); ++iter) {
        delete iter->p_table_mgr;
        iter->p_table_mgr = 0;
    }
}

TableGroup& TableGroup::operator= (const TableGroup& rhs)
{
    if (_table_manager_list.size() == 0) {
        TableRegisteryType::const_iterator iter = 
            rhs._table_manager_list.begin();

        for (;iter != rhs._table_manager_list.end(); ++ iter) {
            table_info_t table_info;
            table_info.p_table_mgr = iter->p_table_mgr->clone(this);
            table_info.table_name = iter->p_table_mgr->desc();
            table_info.inc_level = iter->inc_level;

            register_table_info(table_info);
        }
    } else if (_table_manager_list.size() != rhs._table_manager_list.size()) {
        DL_LOG_FATAL("in operator =, table size not equal,[%lu]!=[%lu]",
                _table_manager_list.size(),rhs._table_manager_list.size());
        return *this;
    }
    
    TableRegisteryType::iterator iter = 
       _table_manager_list.begin();
    TableRegisteryType::const_iterator rhs_iter = 
        rhs._table_manager_list.begin();

    while (iter != _table_manager_list.end() && rhs_iter != rhs._table_manager_list.end()) {
        if (iter->table_name != rhs_iter->table_name) {
            DL_LOG_FATAL("table name not equal,[%s] != [%s]",
                    iter->table_name.c_str(),rhs_iter->table_name.c_str());
            return *this;
        }

        if (iter->p_table_mgr == NULL) {
            DL_LOG_FATAL("Null pointer of iter table mgr,name[%s]",
                    iter->table_name.c_str());
            return *this;
        }

        (*(iter->p_table_mgr)) = *(rhs_iter->p_table_mgr);

        ++iter;
        ++rhs_iter;
    }

    return *this;
}

bool TableGroup::register_table(IBaseTableManager* table,int inc_level)
{
    if (table == NULL) {
        DL_LOG_FATAL("Null pointer of table");
        return false;
    }
    
    if (inc_level <MIN_INC_LEVEL || inc_level >= MAX_INC_LEVEL) {
        DL_LOG_FATAL("Invalid inc level[%d]",inc_level);
        return false;
    }
    
    table_info_t table_info;
    table_info.p_table_mgr = table;
    table_info.table_name = table->desc();
    table_info.inc_level = inc_level;

    register_table_info(table_info); 
    
    return true;
}

bool TableGroup::init()
{
    bool succ = true;

    TableRegisteryType::iterator iter = 
        _table_manager_list.begin();

    for (; iter != _table_manager_list.end(); ++iter) {
         
        bool ret = iter->p_table_mgr->init();
        if (!ret) {
            succ = false;
            DL_LOG_FATAL("Init table[%s] failed",iter->table_name.c_str());
        }
    }

    return succ;
}

bool TableGroup::load()
{
    bool ret = true;
    
    TableRegisteryType::iterator iter;

    for (iter = _table_manager_list.begin(); iter != _table_manager_list.end(); ++iter) {
        ret = iter->p_table_mgr->pre_load();
        if (!ret) {
            DL_LOG_FATAL("Before load table[%s] failed",iter->table_name.c_str());
            break;
        }
    }
   
    for (iter = _table_manager_list.begin(); iter != _table_manager_list.end(); ++iter) {
        ret = iter->p_table_mgr->load();
        if (!ret) {
            DL_LOG_FATAL("Load table[%s] failed",iter->table_name.c_str());
            break;
        }
    }
 
    for (iter = _table_manager_list.begin(); iter != _table_manager_list.end(); ++iter) {
        ret = iter->p_table_mgr->post_load();
        if (!ret) {
            DL_LOG_FATAL("post Load table[%s] failed",iter->table_name.c_str());
            break;
        }
    }
    
    return ret;
}

void TableGroup::serialize()
{
    TableRegisteryType::iterator iter = 
        _table_manager_list.begin();

    for (; iter != _table_manager_list.end(); ++iter) {
        iter->p_table_mgr->serialize();
    }
    
}

bool TableGroup::reload()
{
    return true;
}



bool TableGroup::handle_inc(IncRecordType & inc)
{
    bool ret = true;
    unsigned int level = 0;

    level = DEFAULT_LEVEL;

    IncScheduleInfoType::iterator inc_iter = 
        _inc_schedule_info.find(level);

    if (inc_iter == _inc_schedule_info.end()) {
        DL_LOG_FATAL("No table for handling inc");
        //找不到对应层级增量处理的类
        return true;
    }

    TableRegisteryType::iterator table_iter = inc_iter->second.begin();
    for (; table_iter != inc_iter->second.end(); ++ table_iter) {
        ret |= table_iter->p_table_mgr->update(inc);

        if (!ret) {
            DL_LOG_FATAL("Handle inc for table[%s] failed",table_iter->table_name.c_str());
        }
    }

    return true;
}

const IBaseTableManager* TableGroup::get_table_manager(const std::string& name)
{
    return mutable_table_manager(name);
}

IBaseTableManager* TableGroup::mutable_table_manager(const std::string& name)
{
    TableSeekType::iterator iter = _table_info_map.find(name);

    if (iter == _table_info_map.end()) {
        DL_LOG_FATAL("Failed to find table[%s]",name.c_str());
        return NULL;
    }

    return iter->second.p_table_mgr;
}
    
}//namespace das_lib
