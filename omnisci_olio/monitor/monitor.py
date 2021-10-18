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
try:
    import ibis.backends.omniscidb as ibis_omniscidb
except:
    import ibis.omniscidb as ibis_omniscidb
import ibis
import omnisci_olio.pymapd
import omnisci_olio.ibis

import logging
logging.basicConfig()
log = logging.getLogger('omnisci_monitor')
log.setLevel('DEBUG')


def disk_used(path):
    st = os.statvfs(path)
    # free = st.f_bavail * st.f_frsize
    # total = st.f_blocks * st.f_frsize
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    return (int(used / 1024), 1 - st.f_bavail / st.f_blocks)


def sys_metrics(con):
    df = pd.DataFrame({
        'timestamp_': [pd.datetime.now()],
        'container': con.con._client.get_status(con.con._session)[0].host_name,
        'hostname': os.environ.get('MONITOR_HOSTNAME', None)
    })

    freeout = subprocess.check_output(['free'], text=True).split('\n')
    df['cpu_mem'] = int(freeout[1].split()[2])
    df['cpu_swap'] = int(freeout[2].split()[2])
    
    # topout = subprocess.check_output(['top', '-b', '-n1'], text=True).split('\n')[:5]
    # df['cpu_loadavg'] = float(topout[0].split('load average: ')[1].split(',')[0])
    # cpu_us = topout[2].split('%Cpu(s):')[1].split(',')[0].strip()
    # df['cpu_mem'] = int(topout[3].split(',')[2].strip().split(' ')[0].strip())
    # df['cpu_swap'] = int(topout[4].split(',')[2].strip().split(' ')[0].strip())

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
    names=['timestamp_', 'hostname',
        'cpu_mem', 'cpu_swap', 'cpu_load',
        'disk_used_kb', 'disk_used_pct',
        'gpu_pct_avg', 'gpu_mem_pct_avg', 'gpu_mem_used_mib',
        'gpu_power_draw_w', 'gpu_proc_temp_c',
        'db_cpu_mem_alloc_kb', 'db_cpu_mem_used_kb',
        'db_gpu_mem_alloc_kb', 'db_gpu_mem_used_kb',
        'container',
        ],
    types=['timestamp', 'string', 'float32', 'int64', 'int64',
        'float32', 'int64', 'float32', 'float32', 'float32', 'int32',
        'float32', 'int16', 'int32', 'int32', 'int32', 'int32', 'string'])


def all_metrics(con):
    df = sys_metrics(con)
    gpu = gpu_metrics()
    s = gpu_summary(gpu)
    for k, v in s.items():
        df[k] = v
    
    db = omnisci_olio.ibis.db_memory(con.con, detail=0)

    for i, row in db.iterrows():
        df['db_' + row.device_type + '_mem_alloc_kb'] = int(row.page_size * row.pages_allocated / 1024)
        df['db_' + row.device_type + '_mem_used_kb'] = int(row.page_size * row.used_pages / 1024)
    
    # log.debug('all_metrics df %s', df)
    
    sum_schema.apply_to(df)
    df = df[sum_schema.names]
    # log.debug(f'all_metrics apply_to {df.to_csv()}')
    return {'summary': df, 'gpu': gpu}


# ## Load to OmniSci
# 


summary_table = 'omnisci_system_metrics_summary'


# con.drop_table(summary_table, force=True)


def create_tables(tgt):
    if not summary_table in tgt.list_tables():
        tgt.create_table(summary_table, schema=sum_schema, max_rows=10**9*200)

    # TODO evolve old schema:
    # alter table omnisci_system_metrics_summary add container text


#
# Run Forever
#
def monitor_import(sleep_seconds=1, batch=100, tgt_file=None):
    df = None
    errors = 0
    while True:
        with ibis_omniscidb.connect(os.environ['OMNISCI_DB_URL']) as src:
            while True:
                # log.debug(db_memory(src, detail=0))
                try:
                    df = pd.concat([df, all_metrics(src)['summary']], sort=False)
                    if len(df) >= batch:
                        if tgt_file:
                            with open(tgt_file, 'a') as f:
                                print(df.to_csv(f, header=False))
                        if 'OMNISCI_DB_URL_TGT' in os.environ:
                            with ibis_omniscidb.connect(os.environ['OMNISCI_DB_URL_TGT']) as tgt:
                                create_tables(tgt)
                                tgt.load_data(summary_table, df)
                                t = tgt.table(summary_table)
                                log.info(f'Loaded {len(df)}, count={t.count().execute()}')
                        df = None
                        errors = 0
                    else:
                        sleep(sleep_seconds)
                except Exception as e:
                    errors += 1
                    if errors >= 10:
                        if df:
                            log.error(f'Failed to insert metrics: {df.to_csv()}')
                        raise
                    log.error(f'Continuing after load error: {e} {df}')
                    break # break from the inner while, to re-connect with src
