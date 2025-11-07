#!/bin/bash

sudo modprobe msr

disable_turbo() {
    for cpu in /dev/cpu/[0-9]*; do
        core=${cpu##*/}
        wrmsr -p"$core" 0x1a0 0x4000850089
    done
}

enable_turbo() {
    for cpu in /dev/cpu/[0-9]*; do
        core=${cpu##*/}
        wrmsr -p"$core" 0x1a0 0x850089
    done
}

case "$1" in
    disable)
        disable_turbo
        ;;
    enable)
        enable_turbo
        ;;
    *)
        echo "Usage: $0 {disable|enable}"
        exit 1
        ;;
esac

# usage
#sudo ./disable_turbo.sh disable   # To disable Turbo Boost on all cores
#sudo ./disable_turbo.sh enable    # To enable Turbo Boost on all cores