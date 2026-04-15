import os
import logging

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
        self.amount_by_fruit_by_query = {}
        self.completed_sums_by_query = {}

    def _get_query_amounts(self, query_id):
        if query_id not in self.amount_by_fruit_by_query:
            self.amount_by_fruit_by_query[query_id] = {}
        return self.amount_by_fruit_by_query[query_id]

    def _get_completed_sums(self, query_id):
        if query_id not in self.completed_sums_by_query:
            self.completed_sums_by_query[query_id] = set()
        return self.completed_sums_by_query[query_id]

    def _process_data(self, query_id, fruit, amount):
        logging.info("Processing data message")

        query_amounts = self._get_query_amounts(query_id)
        query_amounts[fruit] = query_amounts.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _emit_partial_top(self, query_id):
        query_amounts = self.amount_by_fruit_by_query.get(query_id, {})
        fruit_items = sorted(query_amounts.values(), reverse=True)
        fruit_items = fruit_items[:TOP_SIZE]

        partial_top = [
            (current_fruit_item.fruit, current_fruit_item.amount)
            for current_fruit_item in fruit_items
        ]

        message = message_protocol.internal.build_partial_top_message(
            query_id,
            ID,
            partial_top,
        )
        self.output_queue.send(message_protocol.internal.serialize(message))

    def _process_eof(self, query_id, source_id):
        logging.info("Processing EOF message")

        completed_sums = self._get_completed_sums(query_id)
        completed_sums.add(source_id)

        if len(completed_sums) < SUM_AMOUNT:
            return

        self._emit_partial_top(query_id)
        self.amount_by_fruit_by_query.pop(query_id, None)
        self.completed_sums_by_query.pop(query_id, None)

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Processing message")
            internal_message = message_protocol.internal.deserialize(message)
            source = message_protocol.internal.get_source(internal_message)

            if message_protocol.internal.is_data_message(internal_message):
                payload = message_protocol.internal.get_payload(internal_message)
                self._process_data(
                    message_protocol.internal.get_query_id(internal_message),
                    payload["fruit"],
                    payload["amount"],
                )
            elif message_protocol.internal.is_eof_message(internal_message):
                self._process_eof(
                    message_protocol.internal.get_query_id(internal_message),
                    source["id"],
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