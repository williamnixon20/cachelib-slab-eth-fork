# slab-rebalance-bench

A detailed version is in [here](https://github.com/eth-easl/slab-rebalance-bench/blob/main/docs/Miss%20ratio%20bench%20mark%20set%20up.md)


**Prerequisites:** You have a cluster of machines (Ubuntu 22.04.) with a large enough shared directory (NFS or any other way). Your Linux user has sudo permission on these machines. We will use one of the machines as the master node, the others as worker nodes.



### Set up the environment
- Clone this repo to your **local** machine
- Configure the machine list: inside the 'slab-rebalance-bench/hosts' directory, there are two files: (1) hosts.txt and (2) username.txt
    - Put the host list inside hosts.txt (we will use the first host as the master node). Follow the xx@xx format.
    - Put your Linux username into username.txt
- After that, run the following command on your local machine:
    ```bash
    cd slab-rebalance-bench/set_up_env && /bin/bash host_init.sh
    ```


### Launch Experiments

1. **SSH into the master node**

2. **Set up the repository on master node:**
   - cd into your NFS shared directory
   - Clone this repo: `git clone <repo-url>`
   - cd into the cloned repository

3. **Configure the machine list again on the master node:**
   - Edit the files in the `slab-rebalance-bench/hosts` directory:
     - Put the host list inside `hosts.txt` (we will use the first host as the master node). Follow the user@hostname format.
     - Put your Linux username into `username.txt`

4. **Configure experiment settings:**
   - cd into slab-rebalance-bench/exp/prepare_exp_configs and run python gen_demo_config.py, this will generate a directory of configurations to run: work_dir_demo
   - Edit `slab-rebalance-bench/exp/configs.json` with the following options:
     - `work_dirs`: `["/path/to/this/repo/exp/work_dir_demo"]` (complete path to your experiment directories)
     - `need_download_traces`: `true` (if you don't have trace files locally)
     - `local_trace_file_dir`: path where you want to store the traces locally (ensure it's NFS shared and has sufficient storage)

5. **Configure host list for master:**
   - cd into `slab-rebalance-bench/exp/master`
   - Edit `hosts.txt` to include your host list (first host will be the master node) 

6. **Launch the master process:**
   ```bash
   cd exp/master
   nohup python3 master.py &
   ```

7. **Monitor progress:**
   After launching `master.py`, a timestamp-named directory (e.g. 20250810_135652) will be created containing:
   - `master.log`: detailed logs of scheduling and job execution
   - `scheduler_state.json`: current state of all experiments
   - `result_processed.csv`: final summary report (generated when all experiments finish)
   
   The `master.log` file provides real-time updates on progress, running jobs, and resource utilization of each host.

8. **Visualizing the result:**
```bash
python exp/plotting/plot_demo_figure.py exp/master/20250816_152551/result_processed.csv meta_202210_kv
```
Remember to replace 20250816_152551 with your timestamp-name directory
Then two figures same as meta_kv_2022210.pdf and meta_kv_202210_rebalanced_slabs.pdf will be generated.

