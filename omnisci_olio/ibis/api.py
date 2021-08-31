import pandas as pd
import ibis


db_details_schema = ibis.schema(
    names=['slab', 'start_page', 'num_pages', 'touch', 'chunk_key', 'buffer_epoch', 'is_free', 'device_type', 'device'],
    types=['int16', 'int32', 'int32', 'int32', 'string', 'int32', 'bool', 'string', 'int16'])


def db_memory(con, detail=1):
    '''
    detail: 0, 1 or 2.
    '''
    reports = {x: con._client.get_memory(con._session, x)
               for x in ['cpu', 'gpu']}

    # print('reports', reports)

    d = []
    for device_type, report in reports.items():
        for i, info in enumerate(report):
            for node in info.node_memory_data:
                x = node.__dict__
                x['device_type'] = device_type
                x['device'] = i
                d.append(x)
    details = pd.DataFrame(d, columns=db_details_schema.names)
    db_details_schema.apply_to(details)
    if detail == 2:
        return details
    
    dfs = []
    for device_type, report in reports.items():
        df = pd.DataFrame([info.__dict__ for info in report])
        df['device_type'] = device_type
        dfs.append(df)

    df = pd.concat(dfs, sort=False)
    df.drop('node_memory_data', axis=1, inplace=True)
    df['device'] = df.index
    df.rename({'host_name': 'hostname',
               'num_pages_allocated': 'pages_allocated',
               'max_num_pages': 'max_pages',
              }, axis=1, inplace=True)

    # if details is empty, the filter returns no columns, so use the full df
    details_not_free = details if len(details) ==0 else details[ ~ details['is_free'] ]
    num_pages = details_not_free.groupby(['device_type', 'device']).agg({'num_pages': 'sum'})
    
    df.set_index(['device_type', 'device'], inplace=True)
    # num_pages.set_index(['device_type', 'device'], inplace=True)
    
    df = df.join(num_pages)
    
    df['used_pages'] = df['num_pages'].fillna(0).astype('int32') # * df['page_size']
    del df['num_pages']

    df.reset_index(inplace=True)
    
    # print('db_memory 1', df)

    if detail == 1:
        df['max_gb'] = df.page_size * df.max_pages / 1024**3
        df['used_gb'] = df.page_size * df.used_pages / 1024**3

        return df
    else:
        df0 = df.groupby(['device_type', 'hostname', 'page_size']).agg({
            'device': 'count', 'max_pages': 'sum',
            'pages_allocated': 'sum', 'used_pages': 'sum'
            })
        df = df0
        df.rename({'device': 'device_count'}, axis=1, inplace=True)
        df.reset_index(inplace=True)
        df['max_gb'] = df.page_size * df.max_pages / 1024**3
        df['used_gb'] = df.page_size * df.used_pages / 1024**3
        # print('db_memory df0', df)
        return df
