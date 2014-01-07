namespace das_lib {

template <class _Table>
TableManager<_Table>::TableManager(const std::string &desc,
                    IBaseLoadStrategy<_Table> *p_load_strategy,
                    IBaseUpdateStrategy<_Table> *p_update_strategy,
                    TableGroup *p_table_group)
        : _desc(desc)
        , _p_load_strategy(p_load_strategy)
        , _p_update_strategy(p_update_strategy)
        , _p_table_group(p_table_group)
{
//        TRACE_LOG("%s", _desc.c_str());
}

template<class _Table>
TableManager<_Table>::~TableManager()
{
//        TRACE_LOG("%s", _desc.c_str());
        
    if (NULL != _p_load_strategy) {
        delete _p_load_strategy;
        _p_load_strategy = NULL;
    }

    if (NULL != _p_update_strategy) {
        delete _p_update_strategy;
        _p_update_strategy = NULL;
    }
        
}

template<class _Table>
const std::string& TableManager<_Table>:: desc() const
{
    return _desc;
}

template<class _Table>
size_t TableManager<_Table>::get_mem() const
{
    return _table.mem(); 
}

template<class _Table>
size_t TableManager<_Table>::table_size() const 
{
    return _table.size();
}

template<class _Table>
_Table * TableManager<_Table>::mutable_table() 
{ 
    return &_table; 
}

template<class _Table>
const _Table &TableManager<_Table>::get_table() const
{
    return _table;
}

template <class _Table>
void TableManager<_Table>::serialize()
{
//    Serializer s;
//    std::string dump_name = "dump_";
    
//    s.set_target((dump_name + _desc + "_" ).c_str());
 
//    dump_table(&_table,s,_desc.c_str());
}

template <class _Table>
TableManager<_Table>::TableManager(const TableManager &rhs)
    : IBaseTableManager(rhs)
    , _table(rhs._table)
    , _desc(rhs._desc)
    , _p_load_strategy(NULL)
    , _p_update_strategy(NULL)
    , _p_table_group(NULL)
{
    //note that we should not copy any pointers here;
}

//note: is meant to be used by version table manager
//so, table name, load & update strategies will not be copied
template <class _Table>
TableManager<_Table> &TableManager<_Table>::operator=(const IBaseTableManager &rhs)
{
    if (this == &rhs) {
        return *this;
    }
    const TableManager<_Table> *p_table_manager = down_cast<const TableManager<_Table> *>(&rhs);
    if (NULL == p_table_manager) {
//        FATAL_LOG("fail to cast table %s, maybe they are of different types", _desc.c_str());
        return *this;
    }

    // only _table needs to be assigned
    _table = p_table_manager->_table;
    
//    TRACE_LOG("%s", _desc.c_str());
    return *this;
}

template <class _Table>
TableManager<_Table> *
TableManager<_Table>::clone(TableGroup *p_table_group) const
{
    TableManager<_Table> *p_tm = new(std::nothrow) TableManager<_Table>(*this);
    if (NULL == p_tm) {
//        FATAL_LOG("fail to allocate TableManager for %s", _desc.c_str());
        return NULL;
    }

    p_tm->_p_table_group = p_table_group;

    if (NULL != _p_load_strategy) {
        p_tm->_p_load_strategy = _p_load_strategy->clone(p_table_group);
        if (NULL == p_tm->_p_load_strategy) {
//            FATAL_LOG("fail to clone load strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }

        if (!p_tm->_p_load_strategy->init(p_tm->_table)) {
//            FATAL_LOG("fail to init load strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }
    }
    if (NULL != _p_update_strategy) {
        p_tm->_p_update_strategy = _p_update_strategy->clone();
        if (NULL == p_tm->_p_update_strategy) {
//            FATAL_LOG("fail to clone update strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }
        if (!p_tm->_p_update_strategy->init(p_tm->_table)) {
//            FATAL_LOG("fail to init update strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }
        
    }

//    TRACE_LOG("cloned a TableManager for %s", _desc.c_str());
    return p_tm;
}


template <class _Table>
bool TableManager<_Table>::init()
{
    int ret = _table.init();
    if (ret < 0) {
//        FATAL_LOG("fail to init table %s", _desc.c_str());
        return false;
    }

    if (NULL != _p_load_strategy) {
        if (!_p_load_strategy->init(_table)) {
//            FATAL_LOG("fail to init load strategy");
            return false;
        }
    }

    if (NULL != _p_update_strategy) {
        if (!_p_update_strategy->init(_table)) {
//            FATAL_LOG("fail to init update strategy");
            return false;
        }
    }

//    TRACE_LOG("TableManager[%s] init ok", _desc.c_str());
    return true;
}

template <class _Table>
bool TableManager<_Table>::load()
{
    if (NULL == _p_load_strategy) {
//        FATAL_LOG("load strategy of %s is NULL", _desc.c_str());
        return false;
    }
    st::Timer tm;
    tm.start();
    bool ret = _p_load_strategy->load(_table);
    if (!ret) {
//        FATAL_LOG("fail to load %s", _desc.c_str());
        return false;
    }
    tm.stop();
//    WARNING_LOG("Loaded %s: %lums (%luns per item)",
//                _desc.c_str(),
//                tm.m_elapsed(),
//                tm.n_elapsed() / never_zero(_table.size()));
    return ret;
}

template <class _Table>
bool TableManager<_Table>::update(const IncRecordType &inc_record)
{
    if (NULL == _p_update_strategy) {
//        FATAL_LOG("update strategy of %s is NULL", _desc.c_str());
        return false;
    }
    return _p_update_strategy->update(_table, inc_record);
}


template <class _Table>
bool TableManager<_Table>::before_load()
{
    if (NULL == _p_load_strategy) {
//        FATAL_LOG("load strategy of %s is NULL", _desc.c_str());
        return false;
    }

    bool ret = _p_load_strategy->before_load(_table);
    if (!ret) {
//        FATAL_LOG("fail to reload %s", _desc.c_str());
        return false;
    }

    return ret;
}

template <class _Table>
bool TableManager<_Table>::after_load()
{
    if (NULL == _p_load_strategy) {
//        FATAL_LOG("load strategy of %s is NULL", _desc.c_str());
        return false;
    }

    bool ret = _p_load_strategy->after_load(_table);
    if (!ret) {
//        FATAL_LOG("fail to reload %s", _desc.c_str());
        return false;
    }

    return ret;
}

template <class _Table>
bool TableManager<_Table>::is_reloaded() const
{
    if (NULL == _p_load_strategy) {
//        FATAL_LOG("load strategy of %s is NULL", _desc.c_str());
        return false;
    }

    return _p_load_strategy->is_reloaded();
}


} //namespace das lib

