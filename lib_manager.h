#ifndef NOVA_BDLIB_MANAGER_H
#define NOVA_BDLIB_MANAGER_H

#include "table_defs.h"
#include "table_group.h"
#include "das_inc_reader.h"

namespace das_lib {

typedef unsigned long long event_id_t;    

class LibManager {
public:
    // It's OK to have default constructor/destructor/copy-constructor/operator=
    LibManager();
    ~LibManager();

    bool init();

    // Load all base index
    bool load_base_indexes(const char *p_path, const char *p_name);

    // Add inc index
    bool handle_inc();

private:
    st::VersionManager<TableGroup> _vm_tg;

    PartitionArg _partition_arg;

    // nebula das inc
    DASIncReader _inc_reader; 
};

}  // namespace afs

#endif  // _BD_EVERYTHING_H_
