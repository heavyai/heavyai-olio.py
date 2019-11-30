#!/usr/bin/env python
# coding: utf-8

# # Monitor CPU and GPU, Load into OmniSci
# 
# TODO add option to load individual records for each cpu and gpu device
#


import os
import sys
import subprocess
import socket
import pandas as pd
from io import StringIO
from time import sleep
import ibis.omniscidb
import ibis
import omnisci_olio.pymapd


import logging
logging.basicConfig()
log = logging.getLogger('default')


def disk_used(path):
    st = os.statvfs(path)
    # free = st.f_bavail * st.f_frsize
    # total = st.f_blocks * st.f_frsize
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    return (int(used / 1024), 1 - st.f_bavail / st.f_blocks)


def sys_metrics():
    df = pd.DataFrame({
        'timestamp_': [pd.datetime.now()],
        # 'hostname': [socket.gethostname()],
        'hostname': os.environ.get('MONITOR_HOSTNAME', None)
    })
    
    topout = subprocess.check_output(['top', '-b', '-n1'], text=True).split('\n')[:5]
    # df['cpu_loadavg'] = float(topout[0].split('load average: ')[1].split(',')[0])
    # cpu_us = topout[2].split('%Cpu(s):')[1].split(',')[0].strip()

    df['cpu_mem'] = int(topout[3].split(',')[2].strip().split(' ')[0].strip())

    df['cpu_swap'] = int(topout[4].split(',')[2].strip().split(' ')[0].strip())
    df

    with open('/proc/loadavg') as f:
        df['cpu_load'] = float(f.read().split(' ')[0])

    x = disk_used('/omnisci-storage')
    df['disk_used_kb'] = x[0]
    df['disk_used_pct'] = x[1]

    return df


def gpu_metrics():
    gpu_cmd = ['nvidia-smi',
           '--format=csv,nounits', # noheader
           '--query-gpu=timestamp,index,uuid,utilization.gpu,utilization.memory,memory.used,memory.free,memory.total,power.draw,temperature.gpu'
            # temperature.memory
           ]
    gpu_line = subprocess.check_output(gpu_cmd, text=True)

    gpu = pd.read_csv(StringIO(gpu_line))
    gpu.timestamp = pd.to_datetime(gpu.timestamp)
    def rename_col(a):
        return a.replace('.','_').strip().replace(' ','_').replace('[','').replace(']','').replace('%','pct').lower()
    gpu.rename(rename_col, axis=1, inplace=True)

    gpu.rename({'timestamp': 'timestamp_',
                'index': 'devicenum',
                'utilization_gpu_pct': 'proc_pct',
                'utilization_memory_pct': 'mem_pct',
                'memory_free_mib': 'mem_free_mib',
                'memory_used_mib': 'mem_used_mib',
                'memory_total_mib': 'mem_total_mib',
                'temperature_gpu': 'proc_temp_c',
               }, axis=1, inplace=True)

    gpu.mem_pct = gpu.mem_used_mib / gpu.mem_total_mib

    return gpu


def gpu_summary(gpu):
    return {'gpu_pct_avg': gpu['proc_pct'].mean(),
     'gpu_mem_pct_avg': gpu['mem_pct'].mean(),
     'gpu_mem_used_mib': gpu['mem_used_mib'].sum(),
     'gpu_power_draw_w': gpu['power_draw_w'].sum(),
     'gpu_proc_temp_c': gpu['proc_temp_c'].max(),
     }


#
# All metrics
# 


sum_schema = ibis.schema(
    names=['timestamp_', 'hostname', 'cpu_mem', 'cpu_swap', 'cpu_load', 'disk_used_kb', 'disk_used_pct',
           'gpu_pct_avg', 'gpu_mem_pct_avg', 'gpu_mem_used_mib', 'gpu_power_draw_w', 'gpu_proc_temp_c',
          'db_cpu_mem_alloc_kb', 'db_cpu_mem_used_kb', 'db_gpu_mem_alloc_kb', 'db_gpu_mem_used_kb'],
    types=['timestamp', 'string', 'float32', 'int64', 'int64', 'float32', 'int64', 'float32', 'float32', 'float32', 'int32', 'float32', 'int16', 'int32', 'int32', 'int32', 'int32'])


def all_metrics(con):
    df = sys_metrics()
    gpu = gpu_metrics()
    s = gpu_summary(gpu)
    for k, v in s.items():
        df[k] = v
    
    db = omnisci_olio.pymapd.db_memory(con.con, detail=0)

    for i, row in db.iterrows():
        df['db_' + row.device_type + '_mem_alloc_kb'] = int(row.page_size * row.pages_allocated / 1024)
        df['db_' + row.device_type + '_mem_used_kb'] = int(row.page_size * row.used_pages / 1024)
    
    # print('all_metrics df', df)
    
    df = sum_schema.apply_to(df)
    return {'summary': df, 'gpu': gpu}


summary_table = 'omnisci_system_metrics_summary'


# con.drop_table(summary_table, force=True)


def create_tables(tgt):
    if not tgt.exists_table(summary_table):
        tgt.create_table(summary_table, schema=sum_schema, max_rows=10**9*200)

#
# Run Forever
#


def monitor_import(sleep_seconds=1, batch=100, tgt_file=None):
    df = None
    errors = 0
    while True:
        with ibis.omniscidb.connect(os.environ['OMNISCI_DB_URL']) as src:
            while True:
                # print(db_memory(src, detail=0))
                try:
                    df = pd.concat([df, all_metrics(src)['summary']], sort=False)
                    if len(df) >= batch:
                        if 'OMNISCI_DB_URL_TGT' in os.environ:
                            with ibis.omniscidb.connect(os.environ['OMNISCI_DB_URL_TGT']) as tgt:
                                create_tables(tgt)
                                tgt.load_data(summary_table, df)
                                t = tgt.table(summary_table)
                                print(f'Loaded {len(df)}, count={t.count().execute()}', file=sys.stderr)
                        if tgt_file:
                            with open(tgt_file, 'a') as f:
                                print(df.to_csv(f, header=False))
                        df = None
                        errors = 0
                    else:
                        sleep(sleep_seconds)
                except Exception as e:
                    errors += 1
                    if errors >= 10:
                        print('Failed to insert metrics: ' + df.to_csv(), file=sys.stderr)
                        raise
                    print(f'Continuing after load error: {e} {df}', file=sys.stderr)
                    break # break from the inner while, to re-connect with src


def main(argv):
    # log.info(argv)
    if argv[0] == 'monitor_import':
        monitor_import(
            int(argv[1]),
            int(argv[2]),
            argv[3] if len(argv) > 3 else None)
    elif argv[0] == 'clear_mem':
        omnisci_olio.pymapd.clear_memory_forever(int(argv[1]))


if __name__ == "__main__":
    main(sys.argv[1:])
