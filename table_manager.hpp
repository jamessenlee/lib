#ifndef DAS_LIB_TABLE_MANAGER_H
#define DAS_LIB_TABLE_MANAGER_H

#include "table_defs.h"
#include "table_strategy.hpp"

// forward declaration for IBaseUpdateStrategy::IncRecordType 
namespace configio {
class DynamicRecord;
}
 
namespace das_lib {

class TableGroup;    
    
class IBaseTableManager {
public:
    virtual ~IBaseTableManager(){}
    virtual bool init() = 0;
    virtual bool before_load() = 0;
    virtual bool load() = 0;
    virtual bool after_load() = 0;
    virtual bool update(const IncRecordType &inc_record) = 0;
    virtual void serialize() = 0;

    //不同版本的TableGroup不同，因此需要在clone时传递
    virtual IBaseTableManager *clone(TableGroup *p_table_group) const = 0;
    virtual IBaseTableManager &operator=(const IBaseTableManager &) = 0;

    virtual bool is_reloaded() const = 0;
    virtual size_t get_mem() const = 0;
    virtual const std::string &desc() const = 0;
    virtual size_t table_size() const = 0;
    
protected:
    
private:

};

template <class _Table>
class TableManager : public IBaseTableManager {
public:
    TableManager(const std::string &desc,
                    IBaseLoadStrategy<_Table> *p_load_strategy,
                    IBaseUpdateStrategy<_Table> *p_update_strategy,
                    TableGroup *p_table_group);
    
    virtual ~TableManager();

    virtual bool init();
    virtual bool before_load();
    virtual bool load();
    virtual bool after_load();
    virtual bool update(const IncRecordType &inc_record);
    virtual TableManager *clone(TableGroup *p_table_group) const;
    virtual TableManager &operator=(const IBaseTableManager &rhs);

    virtual bool is_reloaded() const;
    virtual const std::string &desc();
    virtual size_t get_mem() const;
    virtual size_t table_size() const ;

    _Table *mutable_table();
    const _Table &get_table() const;
    const std::string& desc() const;

    void serialize() ;
    
protected:
    
private:
    TableManager(const TableManager &rhs);
    TableManager &operator=(const TableManager &rhs) {}
    
    _Table _table;  
    const std::string _desc;
    IBaseLoadStrategy<_Table> *_p_load_strategy;     //own this object
    IBaseUpdateStrategy<_Table> *_p_update_strategy; //own this object
    TableGroup *_p_table_group;    //not own this object
};

} //namespace das lib

#include "table_manager_inl.hpp"

#endif
