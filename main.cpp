// Copyright (c) 2010, 2011 Baidu.com, Inc. All Rights Reserved
// Author: GE Jun, JIANG Rujie
#include "new_lib_manager.h"
#include "nova_thread.h"
#include <gflags/gflags.h>

DEFINE_int32(sleep_interval, 1, "Sleep interval after handling inc");
DEFINE_int64(inc_rounds, 2, "Rounds of handling inc, 0 means infinite rounds");
DEFINE_string(bdlib_conf_path, "conf/startup/bdlib", "Set path of config for bdlib");
DEFINE_string(bdlib_conf_name, "afs-bd.conf", "Set name of config for bdlib");
DEFINE_string(index_conf_path, "conf/startup", "Set path of base index config");
DEFINE_string(index_conf_name, "idx_bd.conf", "Set name of base index config");
DEFINE_bool(log_to_screen, true, "Write log on screen or not");
DEFINE_string(log_prefix_name, "mock.",
              "log file is named as <log_prefix_name>.log, "
              "the prefix cannot have underscore and don't miss the .");

DECLARE_string(flagfile);

struct conf_t {
    afs::bd::LibManager bdlib;
};

struct inc_bd_thrd_t : public NOVA_PUBLIC_UTIL::Thread {
    inc_bd_thrd_t(conf_t * conf) : _conf(conf)
    {}
    
    void* run()
    {
        ul_logstat_t ls;
        ls.spec = FLAGS_log_to_screen ? UL_LOGTTY : 0;
        ls.to_syslog = 15;
        ls.events = 15;
        if (ul_openlog_r("inc_thread", &ls) < 0) {
            FATAL_LOG("Fail to open log for inc_thread");
        }

        do {
            int ret = 0;
        
            TRACE_LOG("Begin loading base indexes");
            ret = _conf->bdlib.load_base_indexes(
                FLAGS_index_conf_path.c_str(), FLAGS_index_conf_name.c_str());
            if (ret < 0) {
                FATAL_LOG("failed to load base index!");
                break;
            }
            TRACE_LOG("End loading base indexes");


            system("mv data/word.event.1 data/word.event.1_bak");
            for (long i = 0; 0 == FLAGS_inc_rounds || i < FLAGS_inc_rounds; ++i) {
                //each time
                TRACE_LOG("Begin bdlib.handle_inc, round(%ld)", i);
                ret = _conf->bdlib.handle_inc();
                if(ret < 0) {
                    WARNING_LOG("Error occurred in bdlib.handle_inc()");
                }
                TRACE_LOG("End bdlib.handle_inc");

                if (FLAGS_sleep_interval > 0) {
                    TRACE_LOG("Sleep for %d seconds", FLAGS_sleep_interval);
                    sleep(FLAGS_sleep_interval);
                }
                system("mv data/word.event.1_bak data/word.event.1");
            }
        } while (0);

        ul_closelog_r(0);
        return NULL;
    }
    
    conf_t * _conf;    
};


int main(int argc, char **argv)
{
    int ret = 0;
    conf_t conf;

    FLAGS_flagfile="./conf/startup/gflags.conf";
    ret = google::ParseCommandLineFlags(&argc, &argv, true);
    if (ret < 0) {
        FATAL_LOG("Fail to parse flags");
        return -1;
    }

    ul_logstat_t ls;
    ls.spec = FLAGS_log_to_screen ? UL_LOGTTY : 0;
    ls.to_syslog = 15;
    ls.events = 15;
    if (ul_openlog("./log", FLAGS_log_prefix_name.c_str(), &ls, 100) < 0) {
        FATAL_LOG("Fail to open log");
    }

    if (ret < 0) {
        FATAL_LOG ("Fail to load_conf!");
        return -1;
    }

    ret = conf.bdlib.init ();
    if (! ret ) {
        FATAL_LOG ("Fail to init bdlib!");
        return -1;
    }
    
    TRACE_LOG("Create inc thread");
    inc_bd_thrd_t inc_bd_thrd(&conf);
    inc_bd_thrd.start();
    inc_bd_thrd.join();
        
    ul_closelog(0);
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */