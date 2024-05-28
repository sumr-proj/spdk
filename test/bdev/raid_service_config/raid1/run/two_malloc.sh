#!/bin/bash
# Run with sudo

test_name="two_malloc"

testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../../../../..)
rpc_cfgdir=$(readlink -f $testdir/../rpc_cfg/$test_name)
fio_cfgdir=$(readlink -f $testdir/../fio_cfg/$test_name)

# Test scenario
# crt=$rpc_cfgdir/crt.json

ret=1

function start {
    $rootdir/scripts/rpc.py load_config -j $rpc_cfgdir/crt.json;
    sleep 1;

    $rootdir/scripts/rpc.py ublk_start_disk Raid1 4;
    sleep 1;
}

function action {
    $rootdir/scripts/rpc.py load_config -j $rpc_cfgdir/rfM0.json;
    sleep 1;

    fio $fio_cfgdir/write.fio;
    sleep 1;
    
    # $rootdir/scripts/rpc.py load_config -j $rpc_cfgdir/addM2.json;
    # sleep 5;

    # $rootdir/scripts/rpc.py load_config -j $rpc_cfgdir/rfM1.json;
    # sleep 1;

    fio $fio_cfgdir/verif.fio;
    ret=$?
}

function finish {
    $rootdir/scripts/rpc.py ublk_stop_disk 4;
    sleep 1;

    $rootdir/scripts/rpc.py load_config -j $rpc_cfgdir/dtr.json
    sleep 1;
}

if [ -n "$1" ]
then
    if [ "$1" = "start" ]
    then
        start;
        action;
        exit $(($ret));
    else
        finish;
    fi
else
    start;
    action;
    finish;
    exit $(($ret));
fi
