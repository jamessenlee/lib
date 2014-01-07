namespace das_lib {

template <class _Table>
bool NebulaLoadStrategy<_Table>::init(_Table &)
{   
    return true;
}

template <class _Table>
bool NebulaLoadStrategy<_Table>::before_load(_Table &)
{   
    return true;
}

template <class _Table>
bool NebulaLoadStrategy<_Table>::after_load(_Table &)
{   
    return true;
}

template <class _Table>
bool NebulaLoadStrategy<_Table>::load(_Table &)
{   
    return true;
}

template <class _Table>
bool NebulaLoadStrategy<_Table>::is_reloaded() const
{   
    return true;
}

template <class _Table>
NebulaLoadStrategy<_Table> *
NebulaLoadStrategy<_Table>::clone(TableGroup *) const
{
//    TRACE_LOG("NebulaLoadStrategy clone called");
    return new (std::nothrow) NebulaLoadStrategy<_Table>(*this);
}

//用于倒排的创建
template <class _Table, class _Connector>
ConnectorLoadStrategy<_Table,_Connector>::ConnectorLoadStrategy(const std::string &desc,
            ConnectorMaker connector_maker,
            TableGroup *p_table_group)
         : _conn_desc(desc)
         , _p_connector(NULL)
         , _connector_maker(connector_maker)
         , _p_table_group(p_table_group)
         
{

}

template <class _Table, class _Connector>
ConnectorLoadStrategy<_Table,_Connector>::~ConnectorLoadStrategy()
{
//        TRACE_LOG("%s", _conn_desc.c_str());

    if (NULL != _p_connector) {
        delete _p_connector;
        _p_connector = NULL;
    }
}

template <class _Table, class _Connector>
bool ConnectorLoadStrategy<_Table, _Connector>::is_reloaded() const
{
    return false;
}

template <class _Table, class _Connector>
bool ConnectorLoadStrategy<_Table, _Connector>::load(_Table&)//_Table &table)
{
    return true;
}

template <class _Table, class _Connector>
bool ConnectorLoadStrategy<_Table, _Connector>::after_load(_Table &table)
{
    enable_connector(table);
    return true;
}


template <class _Table, class _Connector>
bool ConnectorLoadStrategy<_Table, _Connector>::before_load(_Table &table)
{
    disable_connector(table);
    return true;
}

template <class _Table, class _Connector>
bool ConnectorLoadStrategy<_Table, _Connector>::init(_Table &table)
{
    if (NULL == _connector_maker || NULL == _p_table_group) {
//        FATAL_LOG("something is NULL [%p/%p]", _connector_maker, _p_table_group);
        return false;
    }

    _p_connector = _connector_maker(table, *_p_table_group);
    if (NULL == _p_connector) {
//        FATAL_LOG("fail to allocate [%s]", _conn_desc.c_str());
        return false;
    }

    show_connector();
    return true;
}

template <class _Table, class _Connector>
void ConnectorLoadStrategy<_Table, _Connector>::show_connector() const 
{
    std::ostringstream oss;
    oss.str("");
    oss << c_show(_Connector);
//    TRACE_LOG("%s con: %s", _conn_desc.c_str(), oss.str().c_str());
}

template <class _Table, class _Connector>
void ConnectorLoadStrategy<_Table, _Connector>::enable_connector(_Table&)//_Table &table)
{
    if (NULL == _p_connector) {
//        FATAL_LOG("%s has not been initialized yet", _conn_desc.c_str());
        return;
    }

    st::Timer tm;
    //const char* const DESC = group_type_desc(_group_type);
    
    tm.start();
    _p_connector->refresh();
    _p_connector->enable_observers();
    
    tm.stop();
//    TRACE_LOG("Enable %s connector(%s)", _conn_desc.c_str(), "DESC to be filled");
//    WARNING_LOG("Refreshed %s(%s): %lums (%luns per item)",
//            _conn_desc.c_str(),
//            "DESC to be filled",
//            tm.m_elapsed(),
//            tm.n_elapsed() / never_zero(table.size()));
}

template <class _Table, class _Connector>
void ConnectorLoadStrategy<_Table, _Connector>::disable_connector(_Table& )//_Table &table)
{
    if (NULL == _p_connector) {
//        FATAL_LOG("%s has not been initialized yet", _conn_desc.c_str());
        return;
    }

//    TRACE_LOG("Disable %s connector(%s)", _conn_desc.c_str(), "DESC to be filled");
            //group_type_desc(_group_type));
    _p_connector->disable_observers();
}

template <class _Table, class _Connector>
ConnectorLoadStrategy<_Table, _Connector>::ConnectorLoadStrategy(const ConnectorLoadStrategy &rhs)
    : IBaseLoadStrategy<_Table>(rhs)
    , _conn_desc(rhs._conn_desc)
    , _p_connector(NULL)
    , _connector_maker(rhs._connector_maker)
    , _p_table_group(NULL)
{
    //note that we should not copy _p_connector and _p_table_group here;
//    TRACE_LOG("%s", _conn_desc.c_str());
}

template <class _Table, class _Connector>
ConnectorLoadStrategy<_Table, _Connector> *
ConnectorLoadStrategy<_Table, _Connector>::clone(TableGroup *p_table_group) const
{
    if (NULL == p_table_group) {
//        FATAL_LOG("p_table_group is NULL");
        return NULL;
    }

    ConnectorLoadStrategy<_Table, _Connector> *p_new_strategy = 
        new (std::nothrow) ConnectorLoadStrategy<_Table, _Connector>(*this);

    if (NULL == p_new_strategy) {
//        FATAL_LOG("fail to allocate %s", _conn_desc.c_str());
        return NULL;
    }

    p_new_strategy->_p_table_group = p_table_group;

//    TRACE_LOG("cloned a ConnectorLoadStrategy for %s", p_new_strategy->_conn_desc.c_str());

    return p_new_strategy;
}

template <class _Table>
bool IncUpdateStrategy<_Table>::init(_Table&)//_Table &table)
{
    return true;
}

template <class _Table>
bool IncUpdateStrategy<_Table>::update(_Table &table, const IncRecordType &record)
{
    if (NULL == _update_handler) {
//        FATAL_LOG("_update_handler is NULL");
        return false;
    }

//    TRACE_LOG("IncUpdateStrategy::update called");
    return _update_handler(table, record);
}

template <class _Table>
IncUpdateStrategy<_Table> *
IncUpdateStrategy<_Table>::clone() const
{
//    TRACE_LOG("IncUpdateStrategy::clone called");
    return new (std::nothrow) IncUpdateStrategy<_Table>(*this);
}

} //namespace
