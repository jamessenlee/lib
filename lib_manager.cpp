#include "new_lib_manager.h"

using st::VersionManager;

namespace afs {
namespace bd {

int parse_test_unit_plan_line(unit_plan_line_t* p_out, char* line_str)
{
    int ret = 0;
    char* str_section[4 + 1];
    const u_int N_SECTION = sizeof(str_section) / sizeof(*str_section);
    ret = split_string(line_str, str_section, N_SECTION, "\t");
    if (ret != 4) {
        WARNING_LOG("Number of fields(%d) is not %u", ret, 4);
        return -1;
    }

    u_int unit_id = 0;
    ret = s2uint(str_section[0], unit_id);
    if (ret < 0) {
        WARNING_LOG("Invalid unit id=%s", str_section[0]);
        return -1;
    }

    u_int plan_id = 0;
    ret = s2uint(str_section[1], plan_id);
    if (ret < 0) {
        WARNING_LOG("Invalid plan_id=%s", str_section[1]);
        return -1;
    }

    p_out->unit_id = unit_id;
    p_out->plan_id = plan_id;
    return 0;

}


int load_test_unit_plan_table(TestUnitPlanTable &table, const char* fpath, const PartitionArg &load_arg)
{
    if (NULL == fpath) {
        FATAL_LOG("Param[fpath] is NULL");
        return -1;
    }

    char line[2048];
    unit_plan_line_t node;
    int ret = 0;
    int line_no = 1;
    int n_succ = 0;
    FILE *fp = fopen(fpath, "r");
    if (NULL == fp) {
        FATAL_LOG("Fail to open file at '%s'", fpath);
        return -1;
    }
    table.clear();
    for (; fgets(line, sizeof(line), fp); ++line_no) {
        line[sizeof(line) - 1] = 0;
        ret = parse_test_unit_plan_line(&node, line);
        if (ret < 0) {
            WARNING_LOG("Ignore line %d", line_no);
            continue;
        }

        if (0 == table.insert(node.unit_id, node.plan_id)) {
            FATAL_LOG("Fail to insert unit_id=%u plan_id=%u into"
                    " TestUnitPlanTable, quit loading",
                    node.unit_id, node.plan_id);
            break;
        }
        ++ n_succ;
    }
    fclose(fp);
    return n_succ;
}


int load_test_unit_table(TestUnitTable &table, const char* fpath, const PartitionArg &load_arg)
{
    if (NULL == fpath) {
        FATAL_LOG("Param[fpath] is NULL");
        return -1;
    }

    return table.insert_by_file(fpath, '\t');
    
}

bool test_update_handler(TestUnitPlanTable &table, const _IncRecordType &record)
{
    static int aaa = 0;
    aaa ++;
    FATAL_LOG("!!!!!!!!!!test_update_handler called aaa[%d]",aaa);
    uint32_t event_id = 0; 
    uint32_t op_type = 0;
    uint32_t unit_id = 0;
    uint32_t plan_id = 0;

    if (0 != get_and_check_uint32(record, 1, "event_id", event_id, 0, MAX_UINT)) {
        FATAL_LOG("Failed to get event_id");
        return -1;
    }
    
    if (0 != get_and_check_uint32(record, 2, "op_type", op_type, 0, 3)) {
        FATAL_LOG("Failed to get op_type");
        return -1;
    }
    
    if (0 != get_and_check_uint32(record, 3, "uint_id", unit_id, 1, MAX_UINT)) {
        FATAL_LOG("Failed to get plan_id");
        return -1;
    }

    if (0 != get_and_check_uint32(record, 4, "plan_id", plan_id, 1, MAX_UINT)) {
        FATAL_LOG("Failed to get plan_id");
        return -1;
    }
    WARNING_LOG("!!![%u],[%u],[%u],[%u]",
            event_id,op_type,unit_id,plan_id);
    

    const bool ERASE_TABLE = (op_type == 2);
    const bool INSERT_TABLE = (op_type == 1);

    // Do changes
    if (ERASE_TABLE) {
        table.erase<UNIT_ID>(unit_id);
    }
    if (INSERT_TABLE) {
        table.insert(unit_id,plan_id);
    }
    
    return true;
}

bool refresh_unit_exp_table(TestUnitTable &table, TableGroup &table_group)
{
    TableManager<TestUnitTable> *p_unit_tm =
            table_group.cast_mutable_table_manager<TableManager<TestUnitTable> >("unit_table");
    if (NULL == p_unit_tm) {
        return false;
    }
    const TestUnitTable &unit_table = p_unit_tm->get_table();
    
    TableManager<TestUnitTable> *p_inverted_tm =
            table_group.cast_mutable_table_manager<TableManager<TestUnitTable> >("inv_unit_table");
    if (NULL == p_inverted_tm) {
        return false;
    }

    //check if reloadable table is reloaded
    if (!p_unit_tm->is_reloaded()) {
        TRACE_LOG("unit table has not been reloaded yet, bail out");
        return true;
    }

    //disconnect inverted table's connector
    bool ret = p_inverted_tm->before_load(); 

    //load exp table
    //clear first
    if (table.empty()) {
        table.resize(unit_table.key_num<UNIT_ID>());
    } else {
        table.clear();
    }
    
    for (TestUnitTable::Iterator it = unit_table.begin(); it; ++it) {
        u_int unit_id = it->at<UNIT_ID>();
        if (unit_id % 2 == 0) {
            table.insert(unit_id);
        }
    }

    //enable connector
    ret = p_inverted_tm->after_load(); 

    return true;
}

TestUnitCon *unit_plan_exp_connector_maker(TestUnitTable &inv_table, TableGroup &table_group)
{
    TestUnitCon *p_connector = new(std::nothrow) TestUnitCon(
            &inv_table,
            table_group.get_table<TestUnitPlanTable>("unit_plan_table"),
            table_group.get_table<TestUnitTable>("unit_exp_table")
        );

    if (NULL == p_connector) {
        printf("p_connector is NULL");
        return NULL;
    }

    p_connector->connect();
    return p_connector;
}


static const char *TEST_UNIT_PLAN_TABLE_PATH = "./test/data/bdlib/test_unit_plan.txt";
static const char *TEST_UNIT_TABLE_PATH = "./test/data/bdlib/test_unit.txt";


    

inline double MB(long mem_in_bytes)
{
    return mem_in_bytes / 1024.0 / 1024.0;
}

LibManager::LibManager()
{}

LibManager::~LibManager()
{}

// Implement Everything
bool LibManager::init()
{
    _partition_arg.set_partition_idx(1).set_partition_num(1);

    int pos = -1;
    int ret = -1;
    
    _vm_tg.init(2,"table_group_version_manager");
    
    pos = _vm_tg.create_version();
    if (pos < 0) {
        FATAL_LOG("Failed to create table group verion");
        return false;
    }

    ///TODO:for test
    TableGroup &table_group = _vm_tg[pos];
    bool ret_b = false;


#define NEW_AND_REGISTER_TABLE(type, name, load_strategy, update_strategy, level, table_group)    \
    TableManager<type> * name =                             \
        new(std::nothrow) TableManager<type>(   #name,              \
                                                load_strategy,      \
                                                update_strategy,    \
                                                &(table_group));      \
    if (NULL == name) {                                     \
        FATAL_LOG("fail to allocate table: "#name);                 \
        return false;                                               \
    }                                                               \
    ret_b = table_group.register_table(name, level);         \
    if (!ret_b) {                                                     \
        FATAL_LOG("fail to register table: "#name);                 \
        return false;                                               \
    }
    

    //add tables
    //reloadable table1
    IBaseLoadStrategy<TestUnitPlanTable> *p_load_strategy = 
        new(std::nothrow) ReloadableLoadStrategy<TestUnitPlanTable>(
                                _partition_arg, 
                                 load_test_unit_plan_table, 
                                 TEST_UNIT_PLAN_TABLE_PATH);
    if(NULL == p_load_strategy) {
        FATAL_LOG("fail to allocate load strategy");
        return false;
    }

    IBaseUpdateStrategy<TestUnitPlanTable>* p_update_strategy 
        = new(std::nothrow) IncUpdateStrategy<TestUnitPlanTable>(test_update_handler);
    

    NEW_AND_REGISTER_TABLE(TestUnitPlanTable, unit_plan_table, p_load_strategy, p_update_strategy, 
                           DEFAULT_LEVEL, table_group);
    
    //reloadable base table
    IBaseLoadStrategy<TestUnitTable> *p_test_unit_load_strategy = 
        new(std::nothrow) ReloadableLoadStrategy<TestUnitTable>(
                                _partition_arg, 
                                 load_test_unit_table, 
                                 TEST_UNIT_TABLE_PATH);
    if(NULL == p_test_unit_load_strategy) {
        FATAL_LOG("fail to allocate load strategy");
        return false;
    }

    NEW_AND_REGISTER_TABLE(TestUnitTable, unit_table, p_test_unit_load_strategy, NULL, 
                           RELOAD_LEVEL, table_group);

    //refreshable exp table
    p_test_unit_load_strategy = 
        new(std::nothrow) RefreshLoadStrategy<TestUnitTable>(
                                refresh_unit_exp_table,
                                &table_group);
    if(NULL == p_test_unit_load_strategy) {
        FATAL_LOG("fail to allocate load strategy");
        return false;
    }

    NEW_AND_REGISTER_TABLE(TestUnitTable, unit_exp_table, p_test_unit_load_strategy, NULL, 
                           RELOAD_LEVEL, table_group);


    // inv table
    p_test_unit_load_strategy = 
        new(std::nothrow) ConnectorLoadStrategy<TestUnitTable, TestUnitCon>(
                                "inv_unit_con", 
                                 unit_plan_exp_connector_maker, 
                                 &table_group);
    if(NULL == p_test_unit_load_strategy) {
        FATAL_LOG("fail to allocate load strategy");
        return false;
    }

    NEW_AND_REGISTER_TABLE(TestUnitTable, inv_unit_table, p_test_unit_load_strategy, NULL, 
                           RELOAD_LEVEL, table_group);


    ret = _vm_tg[pos].init();
    if (ret < 0) {
        FATAL_LOG("Init table group version manager failed");
        return false;
    }
    
    _vm_tg.freeze_version(pos);

    nova::bs::normal_inc_conf_t budget_inc_conf;
    budget_inc_conf.configio_xml_path = "./test_lib.xml";
    budget_inc_conf.last_event_id = 0;
    budget_inc_conf.max_lines_per_round = 1000;
    budget_inc_conf.pipe_name = "aaa";

    
    if (0 != _inc_readers.subscribe_topic(
                    "budget_user", budget_inc_conf)) {
        FATAL_LOG("fail to init das_inc_reader");
        return false;
    }
    return true;
}

bool LibManager::load_base_indexes(const char *p_path, const char *p_name)
{
    int pos = -1;
    pos = _vm_tg.create_version();
    if (pos < 0) {
        FATAL_LOG("Failed to create table group verion");
        return false;
    }

    _vm_tg[pos].load();

    _vm_tg[pos].serialize();

    _vm_tg.freeze_version(pos);

    return true;
}

bool LibManager::handle_inc()
{
    int ret = 0;
    st::Timer tm;

    event_id_t last_event_id = 0;
    event_id_t cur_eid = 0;
    uint32_t count = 0;
    configio::DynamicRecord record;

    ret = _inc_readers.read_next(record);
    if (ret < 0) {
        FATAL_LOG("fail to read next budget inc");
        return false;
    } else if (nova::bs::IndexReader::READ_END == ret) {
        WARNING_LOG("No DAS inc to handle since last round");
        return true;
    }
    TableGroup* p_table = NULL;
    WARNING_LOG("Begin creating new version of main_table_group");
    tm.start();

    int pos = _vm_tg.create_version();
    if (pos < 0) {
        FATAL_LOG("Fail to create a new version of main_table_group");
        return false;
    }
    p_table = &_vm_tg[pos];

    tm.stop();
    WARNING_LOG("End creating new version of main_table_group: %lums",
            tm.m_elapsed());

    tm.start();
    do {
        // get current event id.
        count ++;
        if (0 != get_and_check_uint64(record, INC_EVENTID_IDX, "event_id", cur_eid)) {
            return -1;
        }

        ret = p_table->handle_inc(record);
        if (ret < 0) {
            FATAL_LOG("fail to handle_inc, event_id=%llu", cur_eid);
            return -1;
        }
        last_event_id = cur_eid;

        // get next record
        ret = _inc_readers.read_next(record);
        if (ret < 0) {
            FATAL_LOG("fail to read next budget inc");
            return -1;
        } else if (nova::bs::IndexReader::READ_END == ret) {
            break;
        }

    } while (true);

    _vm_tg[pos].serialize();

    _vm_tg.freeze_version(pos);

    return ret;
}


}  // namespace bd
}  // namespace afs
