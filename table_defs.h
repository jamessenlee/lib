#ifndef DAS_LIB_TABLE_DEF_H
#define DAS_LIB_TABLE_DEF_H

#include <smalltable2.hpp>

namespace das_lib{


DEFINE_ATTRIBUTE(USER_ID, u_int);
DEFINE_ATTRIBUTE(PLAN_ID, u_int);
DEFINE_ATTRIBUTE(UNIT_ID, u_int);

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




}//namespace das lib

#endif
