import os
import logging
import bisect
import signal

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
        self.fruit_top_by_client = {}
        self.eof_sums_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_fruit_item = fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(fruit_top, updated_fruit_item)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id, sum_id):
        eof_sums = self.eof_sums_by_client.setdefault(client_id, set())
        eof_sums.add(sum_id)
        if len(eof_sums) < SUM_AMOUNT:
            logging.info(
                "Received EOF from sum %s for client %s (%s/%s)",
                sum_id,
                client_id,
                len(eof_sums),
                SUM_AMOUNT,
            )
            return

        logging.info("Received EOF from all sums for client %s", client_id)
        self.eof_sums_by_client.pop(client_id, None)
        fruit_top = self.fruit_top_by_client.pop(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        result = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, ID, result])
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            self._process_eof(*fields)
        else:
            raise ValueError(f"Unexpected aggregation message: {fields}")
        ack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        finally:
            for middleware_object in (self.input_exchange, self.output_queue):
                try:
                    middleware_object.close()
                except Exception as exc:
                    logging.warning("Error closing middleware connection: %s", exc)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM; stopping aggregation %s", ID)
        try:
            self.input_exchange.stop_consuming()
        except Exception as exc:
            logging.warning("Could not stop aggregation consumer: %s", exc)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, aggregation_filter.handle_sigterm)
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
