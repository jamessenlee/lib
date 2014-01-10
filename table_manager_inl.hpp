#include "lib_util.h"
#include "lib_dump.hpp"

namespace das_lib {

template <class Table>
TableManager<Table>::TableManager(const std::string &desc,
                    IBaseLoadStrategy<Table> *p_load_strategy,
                    IBaseUpdateStrategy<Table> *p_update_strategy,
                    TableGroup *p_table_group)
        : _desc(desc)
        , _p_load_strategy(p_load_strategy)
        , _p_update_strategy(p_update_strategy)
        , _p_table_group(p_table_group)
{

}

template<class Table>
TableManager<Table>::~TableManager()
{
    if (NULL != _p_load_strategy) {
        delete _p_load_strategy;
        _p_load_strategy = NULL;
    }

    if (NULL != _p_update_strategy) {
        delete _p_update_strategy;
        _p_update_strategy = NULL;
    }
        
}

template<class Table>
void TableManager<Table>::set_table_group(TableGroup* tg)
{
    _p_table_group = tg;
}

template<class Table>
const std::string& TableManager<Table>::desc() const
{
    return _desc;
}

template<class Table>
size_t TableManager<Table>::get_mem() const
{
    return _table.mem(); 
}

template<class Table>
size_t TableManager<Table>::table_size() const 
{
    return _table.size();
}

template<class Table>
Table * TableManager<Table>::mutable_table() 
{ 
    return &_table; 
}

template<class Table>
const Table &TableManager<Table>::get_table() const
{
    return _table;
}

template <class Table>
void TableManager<Table>::serialize()
{
    std::ofstream ofs;
    
    std::string file_prefix("dump_");
    std::string file_name = file_prefix + _desc;
    
    ofs.open(file_name.c_str() );                                          
    if( !ofs.is_open()  ) {                                                
        DL_LOG_FATAL("dump open file failed, file:%s",file_name.c_str()); 
        return ;
    }
    
    dump_table(&_table,ofs,_desc.c_str());

    ofs.close();
}

template <class Table>
TableManager<Table>::TableManager(const TableManager &rhs)
    : IBaseTableManager(rhs)
    , _table(rhs._table)
    , _desc(rhs._desc)
    , _p_load_strategy(NULL)
    , _p_update_strategy(NULL)
    , _p_table_group(NULL)
{
    //note that we should not copy any pointers here;
}

template <class Table>
TableManager<Table> &TableManager<Table>::operator=(const TableManager &rhs)
{
 
}

//note: is meant to be used by version table manager
//so, table name, load & update strategies will not be copied
template <class Table>
TableManager<Table> &TableManager<Table>::operator=(const IBaseTableManager &rhs)
{
    if (this == &rhs) {
        return *this;
    }

    const TableManager<Table> *pTable_manager = down_cast<const TableManager<Table> *>(&rhs);
    if (NULL == pTable_manager) {
        DL_LOG_FATAL("fail to cast table %s, maybe they are of different types", _desc.c_str());
        return *this;
    }

    // only Table needs to be assigned
    _table = pTable_manager->_table;
    
    DL_LOG_TRACE("%s", _desc.c_str());
    return *this;
}

template <class Table>
TableManager<Table> *
TableManager<Table>::clone(TableGroup *p_table_group) const
{
    TableManager<Table> *p_tm = new(std::nothrow) TableManager<Table>(*this);
    if (NULL == p_tm) {
        DL_LOG_FATAL("fail to allocate TableManager for %s", _desc.c_str());
        return NULL;
    }

    p_tm->_p_table_group = p_table_group;

    if (NULL != _p_load_strategy) {
        p_tm->_p_load_strategy = _p_load_strategy->clone(p_table_group);
        if (NULL == p_tm->_p_load_strategy) {
            DL_LOG_FATAL("fail to clone load strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }

        if (!p_tm->_p_load_strategy->init(p_tm->_table)) {
            DL_LOG_FATAL("fail to init load strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }
    }
    if (NULL != _p_update_strategy) {
        p_tm->_p_update_strategy = _p_update_strategy->clone();
        if (NULL == p_tm->_p_update_strategy) {
            DL_LOG_FATAL("fail to clone update strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }
        if (!p_tm->_p_update_strategy->init(p_tm->_table)) {
            DL_LOG_FATAL("fail to init update strategy for %s", _desc.c_str());
            delete p_tm;
            return NULL;
        }
        
    }

    DL_LOG_TRACE("cloned a TableManager for %s", _desc.c_str());
    return p_tm;
}


template <class Table>
bool TableManager<Table>::init()
{
    int ret = _table.init();
    if (ret < 0) {
        DL_LOG_FATAL("fail to init table %s", _desc.c_str());
        return false;
    }

    if (NULL != _p_load_strategy) {
        if (!_p_load_strategy->init(_table)) {
            DL_LOG_FATAL("fail to init load strategy");
            return false;
        }
    }

    if (NULL != _p_update_strategy) {
        if (!_p_update_strategy->init(_table)) {
            DL_LOG_FATAL("fail to init update strategy");
            return false;
        }
    }

    DL_LOG_TRACE("TableManager[%s] init ok", _desc.c_str());
    return true;
}

template <class Table>
bool TableManager<Table>::load()
{
    if (NULL == _p_load_strategy) {
        DL_LOG_FATAL("load strategy of %s is NULL", _desc.c_str());
        return false;
    }
    st::Timer tm;
    tm.start();
    bool ret = _p_load_strategy->load(_table);
    if (!ret) {
        DL_LOG_FATAL("fail to load %s", _desc.c_str());
        return false;
    }
    tm.stop();
    DL_LOG_TRACE("Loaded %s: %lums (%luns per item)",
                _desc.c_str(),
                tm.m_elapsed(),
                tm.n_elapsed() / never_zero(_table.size()));
    return ret;
}

template <class Table>
bool TableManager<Table>::update(const IncRecordType &inc_record)
{
    if (NULL == _p_update_strategy) {
        DL_LOG_FATAL("update strategy of %s is NULL", _desc.c_str());
        return false;
    }
    return _p_update_strategy->update(_table, inc_record);
}


template <class Table>
bool TableManager<Table>::pre_load()
{
    if (NULL == _p_load_strategy) {
        DL_LOG_FATAL("load strategy of %s is NULL", _desc.c_str());
        return false;
    }

    bool ret = _p_load_strategy->pre_load(_table);
    if (!ret) {
        DL_LOG_FATAL("fail to reload %s", _desc.c_str());
        return false;
    }

    return ret;
}

template <class Table>
bool TableManager<Table>::post_load()
{
    if (NULL == _p_load_strategy) {
        DL_LOG_FATAL("load strategy of %s is NULL", _desc.c_str());
        return false;
    }

    bool ret = _p_load_strategy->post_load(_table);
    if (!ret) {
        DL_LOG_FATAL("fail to reload %s", _desc.c_str());
        return false;
    }

    return ret;
}

} //namespace das lib

