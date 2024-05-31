#!/bin/bash
# Run with sudo

# $1 - .../spdk (full_path)
# $2 - .../{ublk_drv}  (full_path) 

# If spdk ublk unable
function setup_ublk() {
    ./configure --with-ublk;
    make -j6;
    cd "$1";
    insmod ./ublk_drv.ko;
}

function fio_test_raid1() {

    local rpc_json_path=./test/bdev/raid1_test_config/rpc_json
    local fio_cfg_path=./test/bdev/raid1_test_config/fio_cfg

    echo " ";
    echo "------------> TEST: $1 & $3 :START <------------";
    
    ./scripts/rpc.py load_config -j $rpc_json_path/$1;
    sleep 1;
    ./scripts/rpc.py ublk_start_disk Raid1 1;
    sleep 1;
    fio $fio_cfg_path/$3;

    if [ $? -eq 0 ];
    then
        echo "$1 & $3 test PASSED";
    else
        echo "$1 & $3 test FAILED";
    fi

    ./scripts/rpc.py ublk_stop_disk 1;
    sleep 1;
    ./scripts/rpc.py load_config -j $rpc_json_path/$2;
    sleep 1;

    echo "------------> TEST: $1 & $3 :FINISH <------------";
    echo " ";
}

function start() {
    ./scripts/setup.sh;
    make -j6;
    sleep 1;
    screen -dmS spdk_tgt ./build/bin/spdk_tgt;
    sleep 1;
    ./scripts/rpc.py ublk_create_target;
    sleep 1;
}

function finish() {
    ./scripts/rpc.py ublk_destroy_target;
    screen -S spdk_tgt -X kill;
}

if [ -z "$1" ]
then 
    spdk_path=.;
else
    spdk_path=$1;
fi

if [ -n "$2" ]
then
    cd spdk_path;
    setup_ublk "$2";
fi

cd spdk_path
start;

fio_test_raid1 raid1.json stop.json randwrite.fio;
fio_test_raid1 raid1.json stop.json write.fio;

finish;
