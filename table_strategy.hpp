#ifndef DAS_LIB_TABLE_STRATEGY_H
#define DAS_LIB_TABLE_STRATEGY_H

#include "table_defs.h"
#include "configio.h"

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
    virtual bool pre_load(Table &table) = 0;
    virtual bool load(Table &table) = 0;
    virtual bool post_load(Table &table) = 0;
    virtual IBaseLoadStrategy *clone(TableGroup *p_table_group) const = 0;
};

template <class Table>
IBaseLoadStrategy<Table>::~IBaseLoadStrategy()
{}


template <class Table>
class LiteralBaseLoadStrategy : public IBaseLoadStrategy<Table> {
public:
    typedef bool (*LoadHandler)(Table &table, const configio::DynamicRecord &);

    LiteralBaseLoadStrategy(const std::string& xml,LoadHandler handler);

    LiteralBaseLoadStrategy(const LiteralBaseLoadStrategy& rhs);

    virtual bool init(Table &table);
    virtual bool pre_load(Table &table);
    virtual bool load(Table &table);
    virtual bool post_load(Table &table);
     
    virtual LiteralBaseLoadStrategy *clone(TableGroup *p_table_group) const;
private:
    std::string _file_path;
    std::string _xml;
    configio::InputObject _reader;
    LoadHandler _handler;
};

//用于倒排的创建
template <class Table, class Connector>
class ConnectorLoadStrategy : public IBaseLoadStrategy<Table> {
public:

    typedef Connector *(*ConnectorMaker)(Table &table, TableGroup &table_group);
    ConnectorLoadStrategy(const std::string &desc,
            ConnectorMaker connector_maker,
            TableGroup *p_table_group);

    virtual ~ConnectorLoadStrategy();
    virtual bool init(Table &table);
    virtual bool pre_load(Table &table) ;
    virtual bool load(Table &table);
    virtual bool post_load(Table &table);
    virtual ConnectorLoadStrategy *clone(TableGroup *p_table_group) const;

private:

    ConnectorLoadStrategy(const ConnectorLoadStrategy &rhs);
    void enable_connector(Table &table);
    void disable_connector(Table &table);
    void show_connector() const;

    const std::string _conn_desc;
    Connector *_p_connector;       //own this object
    ConnectorMaker _connector_maker;
    TableGroup *_p_table_group;    // not own this object

};

template <class Table>
class IBaseUpdateStrategy {
public:
    virtual ~IBaseUpdateStrategy() = 0;
                
    virtual bool init(Table &table) = 0;
    virtual bool update(Table &table, const IncRecordType &) = 0;
    virtual IBaseUpdateStrategy *clone() const = 0;
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
