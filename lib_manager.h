#pragma once
#ifndef NOVA_BDLIB_MANAGER_H
#define NOVA_BDLIB_MANAGER_H

#include "table_group.h"
#include "das_inc_reader.h"
#include "index_reader.h"

namespace das_lib {

///TODO:for test only

typedef unsigned long long event_id_t;    

typedef struct {
    u_int unit_id;
    u_int plan_id;
}unit_plan_line_t;

typedef ST_TABLE(
        UNIT_ID, PLAN_ID,
        ST_UNIQUE_KEY(UNIT_ID, ST_CLUSTER_KEY(PLAN_ID))) TestUnitPlanTable;

typedef ST_TABLE(
        UNIT_ID,
        ST_UNIQUE_KEY(UNIT_ID)) TestUnitTable;

typedef ST_CONNECTOR(
            TestUnitTable,
            ST_PICK(TBL1<UNIT_ID>),
            ST_FROM(TestUnitPlanTable, TestUnitTable),
            ST_WHERE(st::eq(TBL1<UNIT_ID>, TBL2<UNIT_ID>))
        ) TestUnitCon;

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
    IndexReader _inc_readers; 
};

}  // namespace afs

#endif  // _BD_EVERYTHING_H_
