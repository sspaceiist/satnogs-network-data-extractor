import os
import pandas as pd
import numpy as np
import struct
import time

start = time.time()
structure = pd.read_json("beacon_structure.json")
structure.set_index('Name', inplace=True)
print(structure["Type"].unique())
dtypes = {'B1': np.bool, 'B2': np.uint8, 'B3': np.uint8, 'U16': np.uint16, 'U32': np.uint32, 'U8': np.uint8, 'D8': np.uint8, 'I16': np.int16, 'I6': np.int8, 'I32': np.int32, 'I8': np.int8, 'F32': np.float32}
structure['Dtype'] = structure['Type'].map(dtypes)
structure.drop(columns=['Type'], inplace=True)

with open('/home/sspace/abhishek/IS1-Satnogs-Data/satnogs-network-data-extractor/5468948_28_data_5468948_2022-02-16T04-52-05', 'rb') as file:
    header = file.read(16)
    d = {}
    tmp_buffer_size = 0
    tmp_buffer = None
    for i in range(len(structure)):
        field = structure.index[i]
        dtype = structure.iloc[i]["Dtype"]
        size = structure.iloc[i]["Size"]
        endian = structure.iloc[i]["Endian"]
        if(size//8 == 0):
            if not tmp_buffer:
                tmp_buffer = int.from_bytes(file.read(1), byteorder='big', signed=False)
                tmp_buffer_size = 8
                if size == 1:
                    value = bool((tmp_buffer|0xfe)&0x01)
                else:
                    value = int.from_bytes(bytes([tmp_buffer & ((1 << size) - 1)]), byteorder='little' if endian=='big' else 'big', signed=False)
                tmp_buffer = tmp_buffer >> size
                tmp_buffer_size -= 1
            else:
                if size == 1:
                    value = bool((tmp_buffer|(0xfe))&0x01)
                else:
                    value = int.from_bytes(bytes([tmp_buffer & ((1 << size) - 1)]), byteorder='little' if endian=='big' else 'big', signed=False)
                tmp_buffer_size -= size
                tmp_buffer = tmp_buffer >> size
                if tmp_buffer_size == 0:
                    tmp_buffer = None
                    tmp_buffer_size = 8
        else:
            data = file.read(size//8)
            if dtype == np.float32:
                value = struct.unpack('<' + 'f' if endian == 'big' else '>' + 'f', data)[0]
            else:
                value = int.from_bytes(data, byteorder='little' if endian=='big' else 'big', signed=np.issubdtype(dtype, np.signedinteger))
        print(field, '\t', dtype, '\t', size, '\t', endian, '\t', value)


# is1_df = pd.DataFrame(columns=structure.index)

# demodulated_path = '/mnt/is1-health/demodulated/'
# for filename in os.listdir(demodulated_path):
#     with open(os.path.join(demodulated_path, filename), 'rb') as file:
#         header = file.read(16)
#         d = {}
#         for field in structure.index:
#             dtype = structure.loc[field, 'Dtype']
#             size = np.dtype(dtype).itemsize
#             endian = structure.loc[field, 'Endian']
#             data = file.read(size)
#             if dtype == np.float32:
#                 number = struct.unpack('<' + 'f' if endian == 'big' else '>' + 'f', data)[0]
#             else:
#                 number = int.from_bytes(data, byteorder='little' if endian=='big' else 'big', signed=np.issubdtype(dtype, np.signedinteger))
#             d[field] = number
#         is1_df = pd.concat([is1_df, pd.DataFrame([d])], ignore_index=True)

# is1_df.to_sql('is1_health_data.db', if_exists='replace', index=False, con='sqlite:///is1_health_data_level2.db')
# end = time.time()
# print(f"Time taken: {end - start} seconds")