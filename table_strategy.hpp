#ifndef DAS_LIB_TABLE_STRATEGY_H
#define DAS_LIB_TABLE_STRATEGY_H

#include "table_defs.h"

// forward declaration for IBaseUpdateStrategy::IncRecordType 
namespace configio {
class DynamicRecord;
}
 
//管理类的前向声明
namespace das_lib {
    class TableGroup;
}

namespace das_lib {

typedef configio::DynamicRecord IncRecordType;

template <class Table>
class IBaseLoadStrategy {
public:
    virtual ~IBaseLoadStrategy() = 0;
    virtual bool init(Table &table) = 0;
    virtual bool before_load(Table &table) = 0;
    virtual bool load(Table &table) = 0;
    virtual bool after_load(Table &table) = 0;
    virtual IBaseLoadStrategy *clone(TableGroup *pTable_group) const = 0;
    virtual bool is_reloaded() const = 0;
};

template <class Table>
IBaseLoadStrategy<Table>::~IBaseLoadStrategy()
{}


template <class Table>
class IBaseUpdateStrategy {
public:
    virtual ~IBaseUpdateStrategy() = 0;
                
    virtual bool init(Table &table) = 0;
    virtual bool update(Table &table, const IncRecordType &) = 0;
    virtual IBaseUpdateStrategy *clone() const = 0;
};

template <class Table>
IBaseUpdateStrategy<Table>::~IBaseUpdateStrategy()
{}

template <class Table>
class NebulaLoadStrategy : public IBaseLoadStrategy<Table> {
public:
    NebulaLoadStrategy()
    {}

    virtual bool init(Table &table);
    virtual bool before_load(Table &table);
    virtual bool load(Table &table);
    virtual bool after_load(Table &table);
     
    virtual NebulaLoadStrategy *clone(TableGroup *pTable_group) const;

    virtual bool is_reloaded() const;
};

//用于倒排的创建
template <class Table, class _Connector>
class ConnectorLoadStrategy : public IBaseLoadStrategy<Table> {
public:

    typedef _Connector *(*ConnectorMaker)(Table &table, TableGroup &table_group);
    ConnectorLoadStrategy(const std::string &desc,
            ConnectorMaker connector_maker,
            TableGroup *pTable_group);

    virtual ~ConnectorLoadStrategy();
    virtual bool init(Table &table);
    virtual bool before_load(Table &table) ;
    virtual bool load(Table &table);
    virtual bool after_load(Table &table);
    virtual ConnectorLoadStrategy *clone(TableGroup *pTable_group) const;
    virtual bool is_reloaded() const;

private:

    ConnectorLoadStrategy(const ConnectorLoadStrategy &rhs);
    void enable_connector(Table &table);
    void disable_connector(Table &table);
    void show_connector() const;

    const std::string _conn_desc;
    _Connector *_p_connector;       //own this object
    ConnectorMaker _connector_maker;
    TableGroup *_pTable_group;    // not own this object

};

template <class Table>
class IncUpdateStrategy : public IBaseUpdateStrategy<Table> {
public:
    typedef bool (*UpdateHandler)(Table &table, const IncRecordType &);
    
    explicit IncUpdateStrategy(UpdateHandler update_handler)
        : _update_handler(update_handler)
    {}

    virtual bool init(Table &table);
    virtual bool update(Table &table, const IncRecordType &);
    virtual IncUpdateStrategy *clone() const;
    
private:
    UpdateHandler _update_handler;

};

} //namespace das-lib

#include "table_strategy_inl.hpp"

#endif
