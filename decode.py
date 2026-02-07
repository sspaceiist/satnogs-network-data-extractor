from inspiresat1 import Inspiresat1
from kaitaistruct import KaitaiStream

pkt = Inspiresat1.from_file(
    "5468948_28_data_5468948_2022-02-16T04-52-05.bin"
)

data = pkt.ax25_frame.payload.ax25_info.ccsds_space_packet.data_section.user_data_field

print(data.__dict__)
