import os
import logging
import threading
import hashlib
import signal

from common import middleware, message_protocol, fruit_item
from checking import PacketManager

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.packet_coordinator = PacketManager(
            MOM_HOST, ID, SUM_AMOUNT, SUM_PREFIX
        )
        self.state_lock = self.packet_coordinator.state_lock
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        amount_by_fruit = self.amount_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _aggregation_index(self, client_id, fruit):
        partition_key = message_protocol.internal.serialize([client_id, fruit])
        fruit_hash = hashlib.sha256(partition_key).digest()
        return int.from_bytes(fruit_hash, "big") % AGGREGATION_AMOUNT

    def _process_eof(self, client_id):
        with self.state_lock:
            if self.packet_coordinator.is_closed_locked(client_id):
                return

            amount_by_fruit = self.amount_by_client.pop(client_id, {})
            for final_fruit_item in amount_by_fruit.values():
                aggregation_index = self._aggregation_index(
                    client_id, final_fruit_item.fruit
                )
                self.data_output_exchanges[aggregation_index].send(
                    message_protocol.internal.serialize(
                        [
                            client_id,
                            final_fruit_item.fruit,
                            final_fruit_item.amount,
                        ]
                    )
                )

            logging.info("Broadcasting EOF message for client %s", client_id)
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize([client_id, ID])
                )
            self.packet_coordinator.finish_client_locked(client_id)

    def process_control_message(self, message, ack, nack):
        self.packet_coordinator.process_control_message(
            message, ack, nack, self._process_eof
        )

    def process_data_messsage(self, message, ack, nack):
        with self.state_lock:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 4:
                client_id, _, fruit, amount = fields
                self._process_data(client_id, fruit, amount)
                self.packet_coordinator.record_processed_packet_locked(client_id)
                ack()
                return
        if len(fields) == 2:
            client_id, eof_packet_id = fields
            self.packet_coordinator.start_coordination(
                client_id, eof_packet_id - 1
            )
        else:
            raise ValueError(f"Unexpected sum input message: {fields}")
        ack()

    def _stop_control_consumer(self):
        try:
            self.packet_coordinator.stop_consuming()
        except Exception as exc:
            logging.warning("Could not stop sum control consumer: %s", exc)

    def start(self):
        control_thread = threading.Thread(
            target=self.packet_coordinator.start_consuming,
            args=(self.process_control_message,),
            name=f"sum-control-{ID}",
            daemon=True,
        )
        control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_data_messsage)
        finally:
            self._stop_control_consumer()
            control_thread.join()
            middleware_objects = [
                self.input_queue,
                *self.data_output_exchanges,
            ]
            for middleware_object in middleware_objects:
                try:
                    middleware_object.close()
                except Exception as exc:
                    logging.warning("Error closing middleware connection: %s", exc)
            try:
                self.packet_coordinator.close()
            except Exception as exc:
                logging.warning("Error closing coordinator connections: %s", exc)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM; stopping sum %s", ID)
        try:
            self.input_queue.stop_consuming()
        except Exception as exc:
            logging.warning("Could not stop sum data consumer: %s", exc)
        self._stop_control_consumer()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, sum_filter.handle_sigterm)
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
