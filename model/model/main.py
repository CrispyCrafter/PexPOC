import time 
from typing import List

from base.ipc import create_shared_memory, read_shared_memory

import pandas as pd
import polars as pl
import pydantic
import numpy as np


class Data(pydantic.BaseModel):
    a: List[int]
    b: List[int]
    c: List[str]


def main():
    timing = {}
    for i in range(0, 8):
        run = {}
        n = 10**i
        print(f"Running for n = {n:,} rows")

        dict_data = {'a': list(range(n)), 'b': list(range(n)), 'c': list('c' for _ in range(n))}
        df = pd.DataFrame(dict_data)
        pdf = pl.DataFrame(dict_data)
        pydantic_data = Data(**dict_data)

        # Create shared memory for DataFrame
        start = time.time()
        shm_name, shm_size, data_type = create_shared_memory(df, 'pandas')
        read_shared_memory(shm_name, shm_size, data_type)
        end = time.time()
        run['pandas'] = end - start

        # Create shared memory for DataFrame
        start = time.time()
        shm_name, shm_size, data_type = create_shared_memory(pdf, 'polars')
        read_shared_memory(shm_name, shm_size, data_type)
        end = time.time()
        run['polars'] = end - start

        # Create shared memory for dictionary
        start = time.time()
        shm_name, shm_size, data_type = create_shared_memory(dict_data, 'dict')
        read_shared_memory(shm_name, shm_size, data_type)
        end = time.time()
        run['dict'] = end - start

        # Create shared memory for pydantic model
        start = time.time()
        shm_name, shm_size, data_type = create_shared_memory(pydantic_data, 'pydantic')
        read_shared_memory(shm_name, shm_size, data_type)
        end = time.time()
        run['pydantic'] = end - start

        timing[n] = run

    timing_df = pd.DataFrame(timing)
    print(timing_df)

if __name__ == '__main__':
    main()