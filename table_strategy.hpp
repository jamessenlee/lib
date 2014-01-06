#ifndef DAS_LIB_TABLE_STRATEGY_H
#define DAS_LIB_TABLE_STRATEGY_H

#include "table_defs.h"

// forward declaration for IBaseUpdateStrategy::_IncRecordType 
namespace configio {
class DynamicRecord;
}
 
//管理类的前向声明
namespace das_lib {
    class TableGroup;
}

namespace das_lib {

template <class _Table>
class IBaseLoadStrategy {
public:
    virtual ~IBaseLoadStrategy() = 0;
    virtual bool init(_Table &table) = 0;
    virtual bool before_load(_Table &table) = 0;
    virtual bool load(_Table &table) = 0;
    virtual bool after_load(_Table &table) = 0;
    virtual IBaseLoadStrategy *clone(_TableGroup *p_table_group) const = 0;
    virtual bool is_reloaded() const = 0;
};

template <class _Table>
IBaseLoadStrategy<_Table>::~IBaseLoadStrategy()
{}


template <class _Table>
class IBaseUpdateStrategy {
public:
    virtual ~IBaseUpdateStrategy() = 0;
                
    virtual bool init(_Table &table) = 0;
    virtual bool update(_Table &table, const _IncRecordType &) = 0;
    virtual IBaseUpdateStrategy *clone() const = 0;
};

template <class _Table>
IBaseUpdateStrategy<_Table>::~IBaseUpdateStrategy()
{}

template <class _Table>
class NebulaLoadStrategy : public IBaseLoadStrategy<_Table> {
public:
    NebulaLoadStrategy()
    {}

    virtual bool init(_Table &table);
    virtual bool before_load(_Table &table);
    virtual bool load(_Table &table);
    virtual bool after_load(_Table &table);
     
    virtual NebulaLoadStrategy *clone(_TableGroup *p_table_group) const;

    virtual bool is_reloaded() const;
};

template <class _Table>
bool NebulaLoadStrategy<_Table>::load(_Table &)
{   
    return true;
}

template <class _Table>
NebulaLoadStrategy<_Table> *
NebulaLoadStrategy<_Table>::clone(_TableGroup *) const
{
//    TRACE_LOG("NebulaLoadStrategy clone called");
    return new (std::nothrow) NebulaLoadStrategy<_Table>(*this);
}

//用于倒排的创建
template <class _Table, class _Connector>
class ConnectorLoadStrategy : public IBaseLoadStrategy<_Table> {
public:

    typedef _Connector *(*ConnectorMaker)(_Table &table, _TableGroup &table_group);
    ConnectorLoadStrategy(const std::string &desc,
            ConnectorMaker connector_maker,
            _TableGroup *p_table_group);

    virtual ~ConnectorLoadStrategy();
    virtual bool init(_Table &table);
    virtual bool before_load(_Table &table) ;
    virtual bool load(_Table &table);
    virtual bool after_load(_Table &table);
    virtual ConnectorLoadStrategy *clone(_TableGroup *p_table_group) const;
    virtual bool is_reloaded() const;

private:

    ConnectorLoadStrategy(const ConnectorLoadStrategy &rhs);
    void enable_connector(_Table &table);
    void disable_connector(_Table &table);
    void show_connector() const;

    const std::string _conn_desc;
    _Connector *_p_connector;       //own this object
    ConnectorMaker _connector_maker;
    _TableGroup *_p_table_group;    // not own this object

};

template <class _Table>
class IncUpdateStrategy : public IBaseUpdateStrategy<_Table> {
public:
    typedef bool (*UpdateHandler)(_Table &table, const _IncRecordType &);
    
    explicit IncUpdateStrategy(UpdateHandler update_handler)
        : _update_handler(update_handler)
    {}

    virtual bool init(_Table &table);
    virtual bool update(_Table &table, const _IncRecordType &);
    virtual IncUpdateStrategy *clone() const;
    
private:
    UpdateHandler _update_handler;

};

} //namespace das-lib

#include "table_strategy_inl.hpp"

#endif
