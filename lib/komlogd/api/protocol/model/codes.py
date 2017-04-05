from enum import Enum, unique

@unique
class Status(int, Enum):
    # status regarding protocol

    PROTOCOL_ERROR                  = 4400
    ACCESS_DENIED                   = 4403

    # status regarding message

    MESSAGE_EXECUTION_OK            = 4200
    MESSAGE_ACCEPTED_FOR_PROCESSING = 4202
    MESSAGE_EXECUTION_DENIED        = 4430
    MESSAGE_EXECUTION_ERROR         = 4501

    # status regarding resources

    RESOURCE_NOT_FOUND              = 4404

    # status regarding server

    INTERNAL_ERROR                  = 4500
    SERVICE_UNAVAILABLE             = 4503

