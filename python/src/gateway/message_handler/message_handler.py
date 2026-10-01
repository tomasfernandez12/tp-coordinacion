from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex
        self.next_packet_id = 1
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        serialized_message = message_protocol.internal.serialize(
            [self.client_id, self.next_packet_id, fruit, amount]
        )
        self.next_packet_id += 1
        return serialized_message

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize(
            [self.client_id, self.next_packet_id]
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 2 or fields[0] != self.client_id:
            return []
        return fields[1]
