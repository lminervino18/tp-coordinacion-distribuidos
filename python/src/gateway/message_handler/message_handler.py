import uuid

from common import message_protocol


class MessageHandler:
    def __init__(self):
        self.query_id = str(uuid.uuid4())
        assert self.query_id

    def serialize_data_message(self, message):
        fruit, amount = message
        assert isinstance(fruit, str)
        assert fruit
        assert isinstance(amount, int)
        assert amount >= 0

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
            [],
        )
        return message_protocol.internal.serialize(internal_message)

    def deserialize_result_message(self, message):
        internal_message = message_protocol.internal.deserialize(message)

        if not message_protocol.internal.is_final_top_message(internal_message):
            return None

        if message_protocol.internal.get_query_id(internal_message) != self.query_id:
            return None

        payload = message_protocol.internal.get_payload(internal_message)
        assert isinstance(payload, list)
        return payload