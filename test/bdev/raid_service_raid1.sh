#!/bin/bash
# Run with sudo

# Dir pathes
testdir=$(readlink -f $(dirname $0))
rootdir=$(readlink -f $testdir/../..)
cfgdir=$(readlink -f $testdir/raid_service_config/raid1)
rundir=$(readlink -f $cfgdir/run)

function start_ublk_tgt() {
    $rootdir/scripts/rpc.py ublk_create_target;
    sleep 1;
    echo "Ublk target has been created"
}

function stop_ublk_tgt() {
    $rootdir/scripts/rpc.py ublk_destroy_target;
    sleep 2;
    echo "stop ublk target";
}

function start_tgt() {
    screen -dmS spdk_tgt $rootdir/build/bin/spdk_tgt;
    sleep 1;
    echo "spdk_tgt has been started"
    start_ublk_tgt;
}

function finish_tgt() {
    stop_ublk_tgt;
    screen -S spdk_tgt -X kill;
    echo "spdk_tgt has been finished"
}

function run_tests() {
    for file in $rundir/*
    do
        if [ -x "$file" ];
        then
            local ret="";

            echo -e "\e[34mTEST(start)\e[0m: \e[36m$file\e[0m"
            "$file" start;

            if [ $? -eq 0 ]
            then
                ret="\e[32mSUCCESS\e[0m"
            else
                ret="\e[31mFAILED\e[0m"
            fi

            "$file" finish;
            echo -e "\e[34mTEST(finish)\e[0m: $ret"
            echo ""
        fi
    done
}

$rootdir/scripts/setup.sh;
echo "Start build process";
cd $rootdir
make -j12;
cd $testdir
sleep 1;


if [ -n "$1" ]
then
    if [ "$1" = "tgt" ]
    then
        start_tgt;
        run_tests;
        finish_tgt;
    fi
else
    start_ublk_tgt;
    run_tests;
    stop_ublk_tgt;
fi