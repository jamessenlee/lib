#ifndef NEW_TABLE_GROUP_H
#define NEW_TABLE_GROUP_H

#include "table_defs.h"                        // types/attributes
#include "table_manager.hpp"

namespace das_lib {

enum IncLevel {
    MIN_INC_LEVEL = 0,
    DEFAULT_LEVEL = MIN_INC_LEVEL,
    RELOAD_LEVEL,
    INVERTED_LEVEL,
    USER_LEVEL,
    PLAN_LEVEL,
    MAX_INC_LEVEL
};

class TableGroup {
public:
    TableGroup();
    TableGroup(const TableGroup& rhs);
    ~TableGroup();
    TableGroup& operator=(const TableGroup& rhs);
    
    bool register_table(IBaseTableManager* table,int inc_level);
    
    bool init();
    
    bool load();

    bool reload();
    
    bool handle_inc(IncRecordType & inc);

    void serialize();

    const IBaseTableManager* get_table_manager(const std::string& name);

    IBaseTableManager* mutable_table_manager(const std::string& name);

    template<typename To>
    To* cast_mutable_table_manager(const std::string& name)
    {
        return down_cast<To*>(mutable_table_manager(name));
    }

    template<typename To>
    const To* cast_const_table_manager(const std::string& name)
    {
        return down_cast<To*>(get_table_manager(name));
    }

    template<typename TableType>
    TableType* get_table(const std::string& name)
    {
        TableManager<TableType> *p_tm =
            down_cast<TableManager<TableType> *>(mutable_table_manager(name));
        if (NULL == p_tm) {
            return NULL;
        }
        return p_tm->mutable_table();
    }
  
private:
    struct table_info_t {
        IBaseTableManager* p_table_mgr;
        std::string table_name;   
        int inc_level;

        bool operator==(const table_info_t& rhs) const
        {
            return table_name == rhs.table_name;
        }
    };

    bool register_table_info(const table_info_t& table_info);
    
    //注册的顺序,对load,reload,update要按照这个顺序执行
    typedef std::vector<table_info_t> TableRegisteryType;
    TableRegisteryType _table_manager_list;

    typedef std::map<std::string,table_info_t> TableSeekType;
    TableSeekType _table_info_map;
    
    //inc level->table_info，为方便按层级处理增量需求
    typedef std::map<int,TableRegisteryType> IncScheduleInfoType;
    IncScheduleInfoType _inc_schedule_info;
};

}  // namespace das_lib

#endif  // NEW_TABLE_GROUP_H
