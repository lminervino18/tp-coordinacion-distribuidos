import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        # State is kept per query to avoid mixing concurrent clients.
        self.amount_by_fruit_by_query = {}

    def _get_query_state(self, query_id):
        if query_id not in self.amount_by_fruit_by_query:
            self.amount_by_fruit_by_query[query_id] = {}
        return self.amount_by_fruit_by_query[query_id]

    def _process_data(self, query_id, fruit, amount):
        logging.info("Processing data message")
        query_state = self._get_query_state(query_id)
        query_state[fruit] = query_state.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, query_id):
        logging.info("Processing EOF message")

        query_state = self.amount_by_fruit_by_query.get(query_id, {})

        for final_fruit_item in query_state.values():
            message = message_protocol.internal.build_message(
                message_protocol.internal.TYPE_DATA,
                query_id,
                message_protocol.internal.ROLE_SUM,
                ID,
                {
                    "fruit": final_fruit_item.fruit,
                    "amount": final_fruit_item.amount,
                },
            )
            serialized_message = message_protocol.internal.serialize(message)

            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(serialized_message)

        eof_message = message_protocol.internal.build_eof_message(
            query_id,
            message_protocol.internal.ROLE_SUM,
            ID,
        )
        serialized_eof = message_protocol.internal.serialize(eof_message)

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(serialized_eof)

        if query_id in self.amount_by_fruit_by_query:
            del self.amount_by_fruit_by_query[query_id]

    def process_data_messsage(self, message, ack, nack):
        try:
            internal_message = message_protocol.internal.deserialize(message)

            if message_protocol.internal.is_data_message(internal_message):
                payload = message_protocol.internal.get_payload(internal_message)
                self._process_data(
                    message_protocol.internal.get_query_id(internal_message),
                    payload["fruit"],
                    payload["amount"],
                )
            elif message_protocol.internal.is_eof_message(internal_message):
                self._process_eof(
                    message_protocol.internal.get_query_id(internal_message)
                )
            else:
                logging.error("Unsupported message type received in sum")
                nack()
                return

            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()