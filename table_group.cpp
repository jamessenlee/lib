#include "new_table_group.h"
#include "bd_conf.h"
#include "bd_table_manager.hpp"

namespace afs {
namespace bd {

TableGroup::TableGroup()
{

};

bool TableGroup::register_table_info(const table_info_t& table_info)
{
    //1.首先加入到管理顺序容器里面
    //2.再按照不同的层级加入到不同的增量处理顺序容器中
    TableRegisteryType::iterator iter = 
        std::find(_table_group_list.begin(),_table_group_list.end(),
            table_info);

    if (iter != _table_group_list.end()) {
        FATAL_LOG("Table[%s] is already registered",table_info.p_table_mgr->desc().c_str());
        return false;
    }
    
    _table_group_list.push_back(table_info);

    TableRegisteryType& inc_table_group = _inc_schedule_info[table_info.inc_level]; 
    inc_table_group.push_back(table_info);
 
    return true;
}

TableGroup::TableGroup(const TableGroup& rhs)
{
    FATAL_LOG("in copy constructor");
    TableRegisteryType::const_iterator iter = 
        rhs._table_group_list.begin();

    for (;iter != rhs._table_group_list.end(); ++ iter) {
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
        _table_group_list.begin();

    for (; iter != _table_group_list.end(); ++iter) {
        delete iter->p_table_mgr;
        iter->p_table_mgr = 0;
    }
}

TableGroup& TableGroup::operator= (const TableGroup& rhs)
{
    if (_table_group_list.size() == 0) {
        TableRegisteryType::const_iterator iter = 
            rhs._table_group_list.begin();

        for (;iter != rhs._table_group_list.end(); ++ iter) {
            table_info_t table_info;
            table_info.p_table_mgr = iter->p_table_mgr->clone(this);
            table_info.table_name = iter->p_table_mgr->desc();
            table_info.inc_level = iter->inc_level;

            register_table_info(table_info);
        }
    } else if (_table_group_list.size() != rhs._table_group_list.size()) {
        FATAL_LOG("in operator =, table size not equal,[%lu]!=[%lu]",
                _table_group_list.size(),rhs._table_group_list.size());
        return *this;
    }
    
    TableRegisteryType::iterator iter = 
       _table_group_list.begin();
    TableRegisteryType::const_iterator rhs_iter = 
        rhs._table_group_list.begin();

    while (iter != _table_group_list.end() && rhs_iter != rhs._table_group_list.end()) {
        if (iter->table_name != rhs_iter->table_name) {
            FATAL_LOG("table name not equal,[%s] != [%s]",
                    iter->table_name.c_str(),rhs_iter->table_name.c_str());
            return *this;
        }

        if (iter->p_table_mgr == NULL) {
            FATAL_LOG("Null pointer of iter table mgr,name[%s]",
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
        FATAL_LOG("Null pointer of table");
        return false;
    }
    
    if (inc_level <MIN_INC_LEVEL || inc_level >= MAX_INC_LEVEL) {
        FATAL_LOG("Invalid inc level[%d]",inc_level);
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
    bool ret = true;

    TableRegisteryType::iterator iter = 
        _table_group_list.begin();

    for (; iter != _table_group_list.end(); ++iter) {
         
        ret |= iter->p_table_mgr->init();
        if (!ret) {
            FATAL_LOG("Init table[%s] failed",iter->table_name.c_str());
        }
    }

    return ret;
}

bool TableGroup::load()
{
    bool ret = true;
    
    TableRegisteryType::iterator iter;

    for (iter = _table_group_list.begin(); iter != _table_group_list.end(); ++iter) {
        ret = iter->p_table_mgr->before_load();
        if (!ret) {
            FATAL_LOG("Before load table[%s] failed",iter->table_name.c_str());
            break;
        }
    }
   
    for (iter = _table_group_list.begin(); iter != _table_group_list.end(); ++iter) {
        ret = iter->p_table_mgr->load();
        if (!ret) {
            FATAL_LOG("Load table[%s] failed",iter->table_name.c_str());
            break;
        }
    }
 
     for (iter = _table_group_list.begin(); iter != _table_group_list.end(); ++iter) {
        ret = iter->p_table_mgr->after_load();
        if (!ret) {
            FATAL_LOG("after Load table[%s] failed",iter->table_name.c_str());
            break;
        }
    }
    
    return ret;
}

void TableGroup::serialize()
{
    TableRegisteryType::iterator iter = 
        _table_group_list.begin();

    for (; iter != _table_group_list.end(); ++iter) {
        iter->p_table_mgr->serialize();
    }
    
}

bool TableGroup::reload()
{
/*
    bool ret = true;

    IncScheduleInfoType::iterator inc_iter = 
        _inc_schedule_info.find(RELOAD_LEVEL);

    if (inc_iter == _inc_schedule_info.end()) {
        //找不到对应层级增量处理的类
        return true;
    }

    TableRegisteryType::iterator table_iter = inc_iter->second.begin();
    for (; table_iter != inc_iter->second.end(); ++ table_iter) {
        ret |= table_iter->p_table_mgr->update(inc);

        if (!ret) {
            FATAL_LOG("Reload table[%s] failed",table_iter->table_name.c_str());
        }
    }
*/
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
        FATAL_LOG("No table for handling inc");
        //找不到对应层级增量处理的类
        return true;
    }

    TableRegisteryType::iterator table_iter = inc_iter->second.begin();
    for (; table_iter != inc_iter->second.end(); ++ table_iter) {
        FATAL_LOG("hahaahah desc[%s]",table_iter->p_table_mgr->desc().c_str());
        ret |= table_iter->p_table_mgr->update(inc);

        if (!ret) {
            FATAL_LOG("Handle inc for table[%s] failed",table_iter->table_name.c_str());
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
    table_info_t table_info;
    table_info.table_name =  name;

    TableRegisteryType::iterator iter = 
        std::find(_table_group_list.begin(),_table_group_list.end(),
                table_info);

    if(iter == _table_group_list.end()) {
        FATAL_LOG("fail to find table manager %s", name.c_str());
        return NULL;
    }
    
    return iter->p_table_mgr;
}
    
#if 0
bool LibManager::init()
{
    int pos = -1;
    int ret = -1;
    
    _vm_tg.init(2,"table_group_version_manager");
    
    pos = _vm_tg.create_version();
    if (pos < 0) {
        FATAL_LOG("Failed to create table group verion");
        return false;
    }

//    _vm_tg[pos].register_table();
//
    ret = _vm_tg[pos].init();
    if (ret < 0) {
        FATAL_LOG("Init table group version manager failed");
    }
    
    _vm_tg.freeze_version(pos);

    return true;
}

bool LibManager::load()
{
    int pos = -1;
    pos = _vm_tg.create_version();
    if (pos < 0) {
        FATAL_LOG("Failed to create table group verion");
        return false;
    }

    _vm_tg[pos].load();

    _vm_tg.freeze_version(pos);

    return true;
}

bool LibManager::handle_inc()
{
    int pos = -1;
    pos = _vm_tg.create_version();
    if (pos < 0) {
        FATAL_LOG("Failed to create table group verion");
        return false;
    }

    _vm_tg[pos].handle_inc(_das_inc);

    _vm_tg.freeze_version(pos);
   

    return true;
}
#endif

}//namespace bd

}//namespace afs
