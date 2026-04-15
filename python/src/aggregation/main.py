import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:
    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top_by_query = {}

    def _get_query_state(self, query_id):
        if query_id not in self.fruit_top_by_query:
            self.fruit_top_by_query[query_id] = []
        return self.fruit_top_by_query[query_id]

    def _process_data(self, query_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self._get_query_state(query_id)

        for index in range(len(fruit_top)):
            if fruit_top[index].fruit == fruit:
                fruit_top[index] = fruit_top[index] + fruit_item.FruitItem(fruit, amount)
                return

        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, query_id):
        logging.info("Processing EOF message")

        fruit_top = self.fruit_top_by_query.get(query_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()

        partial_top = list(
            map(
                lambda current_fruit_item: (
                    current_fruit_item.fruit,
                    current_fruit_item.amount,
                ),
                fruit_chunk,
            )
        )

        message = message_protocol.internal.build_partial_top_message(
            query_id,
            ID,
            partial_top,
        )
        self.output_queue.send(message_protocol.internal.serialize(message))

        if query_id in self.fruit_top_by_query:
            del self.fruit_top_by_query[query_id]

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Processing message")
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
                logging.error("Unsupported message type received in aggregation")
                nack()
                return

            ack()
        except Exception as exc:
            logging.error(exc)
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()