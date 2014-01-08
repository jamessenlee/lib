#ifndef DAS_LIB_TABLE_MANAGER_H
#define DAS_LIB_TABLE_MANAGER_H

#include "table_defs.h"
#include "table_strategy.hpp"

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
    virtual IBaseTableManager *clone(TableGroup *pTable_group) const = 0;
    virtual IBaseTableManager &operator=(const IBaseTableManager &) = 0;

    virtual size_t get_mem() const = 0;
    virtual const std::string &desc() const = 0;
    virtual size_t table_size() const = 0;
    virtual void set_table_group(TableGroup* tg) = 0;
    
protected:
    
private:

};

template <class Table>
class TableManager : public IBaseTableManager {
public:
    TableManager(const std::string &desc,
                    IBaseLoadStrategy<Table> *p_load_strategy,
                    IBaseUpdateStrategy<Table> *p_update_strategy,
                    TableGroup *pTable_group);
    
    virtual ~TableManager();

    virtual bool init();
    virtual bool before_load();
    virtual bool load();
    virtual bool after_load();
    virtual bool update(const IncRecordType &inc_record);
    virtual TableManager *clone(TableGroup *pTable_group) const;
    virtual TableManager &operator=(const IBaseTableManager &rhs);

    virtual const std::string &desc() const;
    virtual size_t get_mem() const;
    virtual size_t table_size() const ;
    virtual void set_table_group(TableGroup* table_group) ;

    Table *mutable_table();
    const Table &get_table() const;

    void serialize() ;
    
protected:
    
private:
    TableManager(const TableManager &rhs);
    TableManager &operator=(const TableManager &rhs) {}
    
    Table _table;  
    const std::string _desc;
    IBaseLoadStrategy<Table> *_p_load_strategy;     //own this object
    IBaseUpdateStrategy<Table> *_p_update_strategy; //own this object
    TableGroup *_p_table_group;    //not own this object
};

} //namespace das lib

#include "table_manager_inl.hpp"

#endif
