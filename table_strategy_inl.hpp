#include "das_lib_log.h"

namespace das_lib {
    
template <class Table>
bool NebulaLoadStrategy<Table>::init(Table &)
{   
    return true;
}

template <class Table>
bool NebulaLoadStrategy<Table>::before_load(Table &)
{   
    return true;
}

template <class Table>
bool NebulaLoadStrategy<Table>::after_load(Table &)
{   
    return true;
}

template <class Table>
bool NebulaLoadStrategy<Table>::load(Table &)
{   
    return true;
}

template <class Table>
bool NebulaLoadStrategy<Table>::is_reloaded() const
{   
    return true;
}

template <class Table>
NebulaLoadStrategy<Table> *
NebulaLoadStrategy<Table>::clone(TableGroup *) const
{
    DL_LOG_TRACE("NebulaLoadStrategy clone called");
    return new (std::nothrow) NebulaLoadStrategy<Table>(*this);
}

//用于倒排的创建
template <class Table, class _Connector>
ConnectorLoadStrategy<Table,_Connector>::ConnectorLoadStrategy(const std::string &desc,
            ConnectorMaker connector_maker,
            TableGroup *pTable_group)
         : _conn_desc(desc)
         , _p_connector(NULL)
         , _connector_maker(connector_maker)
         , _pTable_group(pTable_group)
         
{

}

template <class Table, class _Connector>
ConnectorLoadStrategy<Table,_Connector>::~ConnectorLoadStrategy()
{
    DL_LOG_TRACE("%s", _conn_desc.c_str());

    if (NULL != _p_connector) {
        delete _p_connector;
        _p_connector = NULL;
    }
}

template <class Table, class _Connector>
bool ConnectorLoadStrategy<Table, _Connector>::is_reloaded() const
{
    return false;
}

template <class Table, class _Connector>
bool ConnectorLoadStrategy<Table, _Connector>::load(Table&)//Table &table)
{
    return true;
}

template <class Table, class _Connector>
bool ConnectorLoadStrategy<Table, _Connector>::after_load(Table &table)
{
    enable_connector(table);
    return true;
}


template <class Table, class _Connector>
bool ConnectorLoadStrategy<Table, _Connector>::before_load(Table &table)
{
    disable_connector(table);
    return true;
}

template <class Table, class _Connector>
bool ConnectorLoadStrategy<Table, _Connector>::init(Table &table)
{
    if (NULL == _connector_maker || NULL == _pTable_group) {
        DL_LOG_FATAL("something is NULL [%p/%p]", _connector_maker, _pTable_group);
        return false;
    }

    _p_connector = _connector_maker(table, *_pTable_group);
    if (NULL == _p_connector) {
        DL_LOG_FATAL("fail to allocate [%s]", _conn_desc.c_str());
        return false;
    }

    show_connector();
    return true;
}

template <class Table, class _Connector>
void ConnectorLoadStrategy<Table, _Connector>::show_connector() const 
{
    std::ostringstream oss;
    oss.str("");
    oss << c_show(_Connector);
    DL_LOG_TRACE("%s con: %s", _conn_desc.c_str(), oss.str().c_str());
}

template <class Table, class _Connector>
void ConnectorLoadStrategy<Table, _Connector>::enable_connector(Table &table)
{
    if (NULL == _p_connector) {
        DL_LOG_FATAL("%s has not been initialized yet", _conn_desc.c_str());
        return;
    }

    st::Timer tm;
    //const char* const DESC = group_type_desc(_group_type);
    
    tm.start();
    _p_connector->refresh();
    _p_connector->enable_observers();
    
    tm.stop();
    DL_LOG_TRACE("Enable %s connector(%s)", _conn_desc.c_str(), "DESC to be filled");
    DL_LOG_WARNING("Refreshed %s(%s): %lums (%luns per item)",
            _conn_desc.c_str(),
            "DESC to be filled",
            tm.m_elapsed(),
            tm.n_elapsed() / never_zero(table.size()));
}

template <class Table, class _Connector>
void ConnectorLoadStrategy<Table, _Connector>::disable_connector(Table& )//Table &table)
{
    if (NULL == _p_connector) {
        DL_LOG_FATAL("%s has not been initialized yet", _conn_desc.c_str());
        return;
    }

    DL_LOG_TRACE("Disable %s connector(%s)", _conn_desc.c_str(), "DESC to be filled");
            //group_type_desc(_group_type));
    _p_connector->disable_observers();
}

template <class Table, class _Connector>
ConnectorLoadStrategy<Table, _Connector>::ConnectorLoadStrategy(const ConnectorLoadStrategy &rhs)
    : IBaseLoadStrategy<Table>(rhs)
    , _conn_desc(rhs._conn_desc)
    , _p_connector(NULL)
    , _connector_maker(rhs._connector_maker)
    , _pTable_group(NULL)
{
    //note that we should not copy _p_connector and _pTable_group here;
    DL_LOG_TRACE("%s", _conn_desc.c_str());
}

template <class Table, class _Connector>
ConnectorLoadStrategy<Table, _Connector> *
ConnectorLoadStrategy<Table, _Connector>::clone(TableGroup *pTable_group) const
{
    if (NULL == pTable_group) {
        DL_LOG_FATAL("pTable_group is NULL");
        return NULL;
    }

    ConnectorLoadStrategy<Table, _Connector> *p_new_strategy = 
        new (std::nothrow) ConnectorLoadStrategy<Table, _Connector>(*this);

    if (NULL == p_new_strategy) {
        DL_LOG_FATAL("fail to allocate %s", _conn_desc.c_str());
        return NULL;
    }

    p_new_strategy->_pTable_group = pTable_group;

    DL_LOG_TRACE("cloned a ConnectorLoadStrategy for %s", p_new_strategy->_conn_desc.c_str());

    return p_new_strategy;
}

template <class Table>
bool IncUpdateStrategy<Table>::init(Table&)//Table &table)
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
