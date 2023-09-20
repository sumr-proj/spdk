#!/bin/bash

# before running:
# load the module ublk_drv.ko into the kernel: insmod {...}/ublk_drv.ko
# activate root mode: sudo -i
# activate virtualenv: source {virtualenv dir}/bin/activate
# run scipt from spdk dir: ./configure --with-ublk
# type 'make' to build spdk
# run this script: bash {path to script}/run_test.sh {full path to spdk}

# $1 full path to spdk

# exit codes:
# 1 - path to spdk isn't entered
# 2 - spdk_tgt does't start

spdk=$1

function start() {
    local old_dir=$(pwd);
    cd $spdk;

    ./scripts/setup.sh >/dev/null&
    pid=$!;
    wait "$pid";

    start-stop-daemon -Sbv -n spdk_tgt -x $spdk/build/bin/spdk_tgt;
    sleep 5;
    ./scripts/rpc.py ublk_create_target;

    cd $old_dir;
}

function finish() {
    local old_dir=$(pwd);
    cd $spdk;

    ./scripts/rpc.py ublk_destroy_target;
    start-stop-daemon -Kvx $spdk/build/bin/spdk_tgt;
    cd $old_dir;
}
 
# $1 full path to start json config
# $2 full path to crash base bdev json config
# $3 full path to stop json config
# $4 full path to fio config
function run_test_with_fio() {
    local old_dir=$(pwd);
    
    cd $spdk;
    ./scripts/rpc.py load_config -j $1;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 1;
    fi
    sleep 1;
    ./scripts/rpc.py ublk_start_disk Raid5 1 >/dev/null;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 1;
    fi
    sleep 1;

    fio $4 >/dev/null;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 2
    fi
    
    ./scripts/rpc.py load_config -j $2;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 1;
    fi
    sleep 1;

    fio $4 >/dev/null;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 2
    fi
    
    ./scripts/rpc.py ublk_stop_disk 1;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 1;
    fi
    sleep 1;
    ./scripts/rpc.py load_config -j $3;
    if [[ "$?" != "0" ]]; then
        cd $old_dir;
        return 1;
    fi
    sleep 1;

    cd $old_dir;
    return 0;
}

function run_tests() {
    local dir_sample=$(pwd)/tests/test;
    local state=0;
    local res;
    echo -e "\033[34mtest with using fio results\033[0m";
    for (( i=0; i <= 2; i++ )) do
        cat $dir_sample$i/info.txt;
        echo;
        if [[ "$state" != "0" ]]; then
            echo -e "result: \033[33mskipped\033[0m";
            continue;
        fi
        run_test_with_fio $dir_sample$i/start.json $dir_sample$i/crash_base_bdev.json $dir_sample$i/stop.json $dir_sample$i/write.fio;
        res=$?;
        if [[ "$res" == 0 ]]; then
            echo -e "write result: \033[32mpassed\033[0m";
        else
            echo -e "write result: \033[31mfailed\033[0m";
            state=1
        fi

        if [[ "$state" != "0" ]]; then
            echo -e "randwrite result: \033[33mskipped\033[0m";
            continue;
        fi
        run_test_with_fio $dir_sample$i/start.json $dir_sample$i/crash_base_bdev.json $dir_sample$i/stop.json $dir_sample$i/randwrite.fio;
        res=$?;
        if [[ "$res" == 0 ]]; then
            echo -e "randwrite result: \033[32mpassed\033[0m";
        else
            echo -e "randwrite result: \033[31mfailed\033[0m";
            state=1
        fi
    done
}

if [[ -z "$spdk" ]]; then
    echo "error: path to spdk isn't entered"
    exit 1
fi

start;
echo;

run_tests;

echo;
finish;