from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 2 or fields[0] != self.client_id:
            return []
        return fields[1]
