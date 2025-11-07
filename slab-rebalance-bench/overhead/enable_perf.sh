sudo apt-get update
sudo apt install python3-pip
pip install pandas
sudo apt-get install msr-tools

sudo apt-get update
sudo apt-get install linux-tools-$(uname -r) linux-tools-common

sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'