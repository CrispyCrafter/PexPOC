import json
from io import BytesIO
from multiprocessing import shared_memory

import pandas as pd
import polars as pl
import pyarrow as pa
import dill as pickle

def to_arrow_buffer(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()

def from_arrow_buffer(buffer):
    memory_pool=pa.system_memory_pool()
    with pa.ipc.open_file(buffer, memory_pool=memory_pool) as reader:
        return reader.read_all()

def serialize_data(data):
    match data:
        case pd.DataFrame():
            obj = to_arrow_buffer(pa.Table.from_pandas(data))
            return obj, len(obj)
        case pl.DataFrame():
            obj = to_arrow_buffer(data.to_arrow()) 
            return obj, len(obj)
        case dict():
            obj = json.dumps(data).encode('utf-8')
            return obj, len(obj)
        case _:
            obj = pickle.dumps(data)
            return obj, len(obj)

def deserialize_data(buffer, data_type):
    match data_type:
        case 'pandas':
            table = from_arrow_buffer(buffer)
            return table.to_pandas()
        case 'polars':
            table = from_arrow_buffer(buffer)
            return table.to_pandas()
        case 'dict':
            json_str = bytes(buffer).decode('utf-8')
            return json.loads(json_str)
        case _:
            return pickle.loads(buffer)

class SharedMemoryManager:
    def __init__(self, size=None, name=None, unlink=False):
        self.size = size
        self.name = name
        self.unlink = unlink
        self.shm = None

    def __enter__(self):
        if self.name:  # Accessing existing shared memory
            self.shm = shared_memory.SharedMemory(name=self.name)
        else:  # Creating new shared memory
            self.shm = shared_memory.SharedMemory(create=True, size=self.size)
        return self.shm

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shm.close()
        if self.unlink:  # Unlink only if specified
            self.shm.unlink()

def create_shared_memory(data, data_type):
    serialized_data, size = serialize_data(data)
    with SharedMemoryManager(size=size) as shm:  # Do not unlink here
        shm.buf[:size] = bytes(serialized_data)
        shm_name = shm.name
        shm_size = size
    return shm_name, shm_size, data_type

def read_shared_memory(shm_name, shm_size, data_type):
    with SharedMemoryManager(name=shm_name, unlink=True) as existing_shm:
        buffer = existing_shm.buf[:shm_size]
        data = deserialize_data(buffer, data_type)
        del buffer  # Explicitly release the memory view
    return data

