import uuid

from common import message_protocol


class MessageHandler:
    def __init__(self):
        self.query_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        fruit, amount = message

        internal_message = message_protocol.internal.build_data_message(
            self.query_id,
            fruit,
            amount,
        )

        return message_protocol.internal.serialize(internal_message)

    def serialize_eof_message(self, message):
        internal_message = message_protocol.internal.build_eof_message(
            self.query_id,
            message_protocol.internal.ROLE_GATEWAY,
            None,
        )

        return message_protocol.internal.serialize(internal_message)

    def deserialize_result_message(self, raw_message):
        internal_message = message_protocol.internal.deserialize(raw_message)

        if not message_protocol.internal.is_final_top_message(internal_message):
            return None

        query_id = message_protocol.internal.get_query_id(internal_message)

        if query_id != self.query_id:
            return None

        return message_protocol.internal.get_payload(internal_message)
