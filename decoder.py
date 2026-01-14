import os
import pandas as pd
import numpy as np
import struct
import time

start = time.time()
structure = pd.read_csv('packet_def.csv')
structure.set_index('Name', inplace=True)
structure = structure[structure['APID'] == 1]
structure = structure.drop(columns=['APID'])
dtypes = {'U16': np.uint16, 'U32': np.uint32, 'U8': np.uint8, 'D8': np.uint8, 'I16': np.int16, 'I6': np.int8, 'I32': np.int32, 'I8': np.int8, 'F32': np.float32}
structure['Dtype'] = structure['Type'].map(dtypes)
structure.drop(columns=['Type'], inplace=True)

is1_df = pd.DataFrame(columns=structure.index)

demodulated_path = '/mnt/is1-health/demodulated/'
for filename in os.listdir(demodulated_path):
    with open(os.path.join(demodulated_path, filename), 'rb') as file:
        header = file.read(16)
        d = {}
        for field in structure.index:
            dtype = structure.loc[field, 'Dtype']
            size = np.dtype(dtype).itemsize
            endian = structure.loc[field, 'Endian']
            data = file.read(size)
            if dtype == np.float32:
                number = struct.unpack('<' + 'f' if endian == 'big' else '>' + 'f', data)[0]
            else:
                number = int.from_bytes(data, byteorder='little' if endian=='big' else 'big', signed=np.issubdtype(dtype, np.signedinteger))
            d[field] = number
        is1_df = pd.concat([is1_df, pd.DataFrame([d])], ignore_index=True)

is1_df.to_sql('is1_health_data.db', if_exists='replace', index=False, con='sqlite:///is1_health_data.db')
end = time.time()
print(f"Time taken: {end - start} seconds")