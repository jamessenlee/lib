#pragma once
#ifndef NEW_TABLE_GROUP_H
#define NEW_TABLE_GROUP_H

#include "table_defs.h"                        // types/attributes
#include "table_manager.hpp"

namespace das_lib {

class IBaseTableManager;

template <class _Table>
class TableManager;

template <class _Table, class _Connector>
class InvertedTableManager;

    
// Table definitions:

typedef configio::DynamicRecord IncRecordType;

#if 0
template<typename To, typename From>
inline To implicit_cast(From const &f) {
    return f;
}

template<typename To, typename From>
inline To down_cast(From* f) {
    if (false) {
        implicit_cast<From*, To>(0);
    }
    
#if !defined(NDEBUG)
    assert(f == NULL || dynamic_cast<To>(f) != NULL);  //
#endif
    return static_cast<To>(f);
}
#endif
/*
class IBaseTableManager {
public:
    virtual ~IBaseTableManager() = 0;
    virtual bool init() = 0;
    virtual bool load() = 0;
    virtual bool update(const IncRecordType &inc_record) = 0;
     
    virtual IBaseTableManager *clone() const = 0;
    virtual IBaseTableManager &operator=(const IBaseTableManager &) = 0;
         
    virtual size_t get_mem() const = 0;
    virtual const std::string &desc() const = 0;
        
protected:
         
private:
 
};
*/
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

    template<typename TableType, typename ConnectorType>
    TableType* get_inverted_table(const std::string& name)
    {
        InvertedTableManager<TableType, ConnectorType> *p_tm =
            down_cast<InvertedTableManager<TableType, ConnectorType> *>(mutable_table_manager(name));
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
    TableRegisteryType _table_group_list;
    //level->table_info
    typedef std::map<int,TableRegisteryType> IncScheduleInfoType;
    IncScheduleInfoType _inc_schedule_info;
};

#if 0
class LibManager {
public:
    bool init();
    bool load();
    bool handle_inc();

private:
    IncRecordType _das_inc;
    st::VersionManager<TableGroup> _vm_tg;

};
#endif

}  // namespace das_lib

#endif  // NEW_TABLE_GROUP_H
