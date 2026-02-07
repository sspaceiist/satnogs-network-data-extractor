from inspiresat1 import Inspiresat1

pkt = Inspiresat1.from_file(
    "5468948_28_data_5468948_2022-02-16T04-52-05.bin"
)

def packet_to_dict(pkt):
    ccsds_pkt = pkt.ax25_frame.payload.ax25_info.ccsds_space_packet
    data = ccsds_pkt.data_section
    secondary_header = data.secondary_header
    sh_coarse = secondary_header.sh_coarse
    sh_fine = secondary_header.sh_fine
    dict_data = {
        "sh_coarse": sh_coarse,
        "sh_fine": sh_fine
    }
    user_data = data.user_data_field
    fields = list(user_data.__dict__.keys())[3:]
    for field in fields:
        dict_data[field] = getattr(user_data, field)
    return dict_data

print(packet_to_dict(pkt))
# print(data.__dict__)
