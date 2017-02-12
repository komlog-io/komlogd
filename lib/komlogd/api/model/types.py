from enum import Enum, unique

@unique
class Actions(Enum):
    HOOK_TO_URI             = 'hook_to_uri'
    SEND_MULTI_DATA         = 'send_multi_data'
    SEND_DP_DATA            = 'send_dp_data'
    SEND_DS_DATA            = 'send_ds_data'
    UNHOOK_FROM_URI         = 'unhook_from_uri'
    REQUEST_DATA            = 'request_data'
    SEND_DATA_INTERVAL      = 'send_data_interval'

@unique
class Metrics(Enum):
    DATASOURCE              = 'd'
    DATAPOINT               = 'p'

