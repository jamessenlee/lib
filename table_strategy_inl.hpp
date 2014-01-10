#include "das_lib_log.h"

namespace das_lib {
    
template <class Table>
LiteralBaseLoadStrategy<Table>::LiteralBaseLoadStrategy(const std::string& xml,
        LoadHandler handler)
    : _xml(xml)
    , _handler(handler)
{   

}

template <class Table>
LiteralBaseLoadStrategy<Table>::LiteralBaseLoadStrategy(const LiteralBaseLoadStrategy &rhs)
    : IBaseLoadStrategy<Table>(rhs)
    , _xml(rhs._xml)
    , _handler(rhs._handler)
{   

}
    
template <class Table>
bool LiteralBaseLoadStrategy<Table>::init(Table &)
{   
    int ret = 0;

    ret = _reader.init(_xml.c_str());
    if (0 != ret) {
        DL_LOG_FATAL("fail to init InputObject, xml[%s].",
                _xml.c_str());
        return -1;
    }
    return true;
}

template <class Table>
bool LiteralBaseLoadStrategy<Table>::pre_load(Table &)
{   
    return true;
}

template <class Table>
bool LiteralBaseLoadStrategy<Table>::post_load(Table &)
{   
    return true;
}

template <class Table>
bool LiteralBaseLoadStrategy<Table>::load(Table& table)
{   
    int ret = -1;
    // open 
    ret = _reader.open();
    if (0 != ret) {
        DL_LOG_FATAL("fail to open InputObject, xml[%s].",
                _xml.c_str());
        return -1;
    }

    configio::DynamicRecord record;


    ret = _reader.get_next_record(record);

    while (ret == 0) {

        (*_handler)(table,record);

        ret = _reader.get_next_record(record);
        if (ret < 0) {
            DL_LOG_FATAL("Reading base failed[%s]",_xml.c_str());
            return false;
        }
    }

    _reader.close();
    
    return true;
}

template <class Table>
LiteralBaseLoadStrategy<Table> *
LiteralBaseLoadStrategy<Table>::clone(TableGroup *) const
{
    DL_LOG_TRACE("LiteralBaseLoadStrategy clone called");
    return new (std::nothrow) LiteralBaseLoadStrategy<Table>(*this);
}

//用于倒排的创建
template <class Table, class Connector>
ConnectorLoadStrategy<Table,Connector>::ConnectorLoadStrategy(const std::string &desc,
            ConnectorMaker connector_maker,
            TableGroup *p_table_group)
         : _conn_desc(desc)
         , _p_connector(NULL)
         , _connector_maker(connector_maker)
         , _p_table_group(p_table_group)
         
{

}

template <class Table, class Connector>
ConnectorLoadStrategy<Table,Connector>::~ConnectorLoadStrategy()
{
    DL_LOG_TRACE("%s", _conn_desc.c_str());

    if (NULL != _p_connector) {
        delete _p_connector;
        _p_connector = NULL;
    }
}

template <class Table, class Connector>
bool ConnectorLoadStrategy<Table, Connector>::load(Table&)//Table &table)
{
    return true;
}

template <class Table, class Connector>
bool ConnectorLoadStrategy<Table, Connector>::post_load(Table &table)
{
    enable_connector(table);
    return true;
}


template <class Table, class Connector>
bool ConnectorLoadStrategy<Table, Connector>::pre_load(Table &table)
{
    disable_connector(table);
    return true;
}

template <class Table, class Connector>
bool ConnectorLoadStrategy<Table, Connector>::init(Table &table)
{
    if (NULL == _connector_maker || NULL == _p_table_group) {
        DL_LOG_FATAL("something is NULL [%p/%p]", _connector_maker, _p_table_group);
        return false;
    }

    _p_connector = _connector_maker(table, *_p_table_group);
    if (NULL == _p_connector) {
        DL_LOG_FATAL("fail to allocate [%s]", _conn_desc.c_str());
        return false;
    }

    show_connector();
    return true;
}

template <class Table, class Connector>
void ConnectorLoadStrategy<Table, Connector>::show_connector() const 
{
    std::ostringstream oss;
    oss.str("");
    oss << c_show(Connector);
    DL_LOG_TRACE("%s con: %s", _conn_desc.c_str(), oss.str().c_str());
}

template <class Table, class Connector>
void ConnectorLoadStrategy<Table, Connector>::enable_connector(Table &table)
{
    if (NULL == _p_connector) {
        DL_LOG_FATAL("%s has not been initialized yet", _conn_desc.c_str());
        return;
    }

    st::Timer tm;
    
    tm.start();
    _p_connector->refresh();
    _p_connector->enable_observers();
    
    tm.stop();
    DL_LOG_TRACE("Enable %s connector(%s)", _conn_desc.c_str(), "DESC to be filled");
    DL_LOG_TRACE("Refreshed %s(%s): %lums (%luns per item)",
            _conn_desc.c_str(),
            "DESC to be filled",
            tm.m_elapsed(),
            tm.n_elapsed() / never_zero(table.size()));
}

template <class Table, class Connector>
void ConnectorLoadStrategy<Table, Connector>::disable_connector(Table& )
{
    if (NULL == _p_connector) {
        DL_LOG_FATAL("%s has not been initialized yet", _conn_desc.c_str());
        return;
    }

    DL_LOG_TRACE("Disable %s connector(%s)", _conn_desc.c_str(), "DESC to be filled");
    _p_connector->disable_observers();
}

template <class Table, class Connector>
ConnectorLoadStrategy<Table, Connector>::ConnectorLoadStrategy(const ConnectorLoadStrategy &rhs)
    : IBaseLoadStrategy<Table>(rhs)
    , _conn_desc(rhs._conn_desc)
    , _p_connector(NULL)
    , _connector_maker(rhs._connector_maker)
    , _p_table_group(NULL)
{
    //note that we should not copy _p_connector and _p_table_group here;
    DL_LOG_TRACE("%s", _conn_desc.c_str());
}

template <class Table, class Connector>
ConnectorLoadStrategy<Table, Connector> *
ConnectorLoadStrategy<Table, Connector>::clone(TableGroup *p_table_group) const
{
    if (NULL == p_table_group) {
        DL_LOG_FATAL("p_table_group is NULL");
        return NULL;
    }

    ConnectorLoadStrategy<Table, Connector> *p_new_strategy = 
        new (std::nothrow) ConnectorLoadStrategy<Table, Connector>(*this);

    if (NULL == p_new_strategy) {
        DL_LOG_FATAL("fail to allocate %s", _conn_desc.c_str());
        return NULL;
    }

    p_new_strategy->_p_table_group = p_table_group;

    DL_LOG_TRACE("cloned a ConnectorLoadStrategy for %s", p_new_strategy->_conn_desc.c_str());

    return p_new_strategy;
}

template <class Table>
IBaseUpdateStrategy<Table>::~IBaseUpdateStrategy()
{}

template <class Table>
bool IncUpdateStrategy<Table>::init(Table&)
{
    return true;
}

template <class Table>
bool IncUpdateStrategy<Table>::update(Table &table, const IncRecordType &record)
{
    if (NULL == _update_handler) {
        DL_LOG_FATAL("_update_handler is NULL");
        return false;
    }

    DL_LOG_TRACE("IncUpdateStrategy::update called");
    return _update_handler(table, record);
}

template <class Table>
IncUpdateStrategy<Table> *
IncUpdateStrategy<Table>::clone() const
{
    DL_LOG_TRACE("IncUpdateStrategy::clone called");
    return new (std::nothrow) IncUpdateStrategy<Table>(*this);
}

} //namespace
