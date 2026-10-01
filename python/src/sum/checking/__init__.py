import threading

from common import middleware, message_protocol


class PacketManager:

    def __init__(self, host, sum_id, sum_amount, sum_prefix):
        """Inicializa el estado de coordinación y sus exchanges RabbitMQ."""
        self.sum_id = sum_id
        self.sum_amount = sum_amount
        self.exchange_name = f"{sum_prefix}_control_exchange"
        self.eof_routing_key = f"{sum_prefix}_eof"
        self.response_routing_prefix = f"{sum_prefix}_response"
        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host, self.exchange_name, [self.eof_routing_key]
        )
        self.control_start_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host, self.exchange_name, [self.eof_routing_key]
        )
        self.control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host,
            self.exchange_name,
            [
                self.eof_routing_key,
                f"{self.response_routing_prefix}_{sum_id}",
            ],
        )
        self.control_response_exchanges = [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                host,
                self.exchange_name,
                [f"{self.response_routing_prefix}_{target_sum_id}"],
            )
            for target_sum_id in range(sum_amount)
        ]
        self.processed_packets_by_client = {}
        self.count_reports_by_client_and_round = {}
        self.coordinations_by_client = {}
        self.closed_clients = set()
        self.state_lock = threading.Lock()

    def record_processed_packet_locked(self, client_id):
        """Incrementa el conteo local de paquetes procesados por un cliente.
        """

        self.processed_packets_by_client[client_id] = (
            self.processed_packets_by_client.get(client_id, 0) + 1
        )

    def is_closed_locked(self, client_id):
        """Indica si la coordinación del cliente ya fue finalizada.
        """

        return client_id in self.closed_clients

    def finish_client_locked(self, client_id):
        """Elimina el estado del cliente y lo marca como finalizado.
        """

        self.processed_packets_by_client.pop(client_id, None)
        self.count_reports_by_client_and_round.pop(client_id, None)
        self.coordinations_by_client.pop(client_id, None)
        self.closed_clients.add(client_id)

    def start_coordination(self, client_id, total_packets):
        """Inicia el conteo distribuido, salvo que ya esté iniciado o cerrado.
        """

        with self.state_lock:
            if (
                client_id in self.closed_clients
                or client_id in self.coordinations_by_client
            ):
                return
            self.coordinations_by_client[client_id] = {
                "remaining": total_packets,
                "round_id": 0,
                "reports": {},
                "committing": False,
            }

        self.control_start_exchange.send(
            message_protocol.internal.serialize(
                ["COUNT_REQUEST", client_id, total_packets, self.sum_id, 0]
            )
        )

    def _report_processed_count(self, client_id, coordinator_id, round_id):
        """Publica el conteo local correspondiente a una ronda solicitada para un cliente.
        Reutiliza el conteo si llega otra solicitud de la misma ronda e ignora
        solicitudes de rondas anteriores.
        """

        with self.state_lock:
            previous_report = self.count_reports_by_client_and_round.get(client_id)
            if previous_report is not None and previous_report[0] == round_id:
                processed_packets = previous_report[1]
            elif previous_report is not None and round_id < previous_report[0]:
                return
            else:
                processed_packets = self.processed_packets_by_client.pop(
                    client_id, 0
                )
                self.count_reports_by_client_and_round[client_id] = (
                    round_id,
                    processed_packets,
                )

        report = message_protocol.internal.serialize(
            ["COUNT_REPORT", client_id, self.sum_id, round_id, processed_packets]
        )
        self.control_response_exchanges[coordinator_id].send(report)

    def _process_report(self, client_id, sum_id, round_id, processed_packets):
        """Registra un reporte y avanza la coordinación cuando están todos.

        Al completar una ronda, solicita otra si quedan paquetes por contar,de lo contrario, publica el commit.
        """
        next_request = None
        should_commit = False
        with self.state_lock:
            coordination = self.coordinations_by_client.get(client_id)
            if (
                coordination is None
                or coordination["committing"]
                or coordination["round_id"] != round_id
                or sum_id in coordination["reports"]
            ):
                return

            coordination["reports"][sum_id] = processed_packets
            if len(coordination["reports"]) == self.sum_amount:
                total_processed = sum(coordination["reports"].values())
                coordination["remaining"] -= total_processed
                if coordination["remaining"] <= 0:
                    coordination["committing"] = True
                    should_commit = True
                else:
                    coordination["round_id"] += 1
                    coordination["reports"] = {}
                    next_request = (
                        coordination["remaining"],
                        coordination["round_id"],
                    )

        if should_commit:
            self.control_output_exchange.send(
                message_protocol.internal.serialize(["COMMIT", client_id])
            )
        elif next_request is not None:
            self._publish_count_request(client_id, *next_request)

    def _publish_count_request(self, client_id, remaining_packets, round_id):
        """Publica una solicitud de conteo para la ronda indicada.
        """

        self.control_output_exchange.send(
            message_protocol.internal.serialize(
                [
                    "COUNT_REQUEST",
                    client_id,
                    remaining_packets,
                    self.sum_id,
                    round_id,
                ]
            )
        )

    def process_control_message(self, message, ack, nack, on_commit):
        fields = message_protocol.internal.deserialize(message)
        message_type = fields[0]
        if message_type == "COUNT_REQUEST":
            _, client_id, _, coordinator_id, round_id = fields
            self._report_processed_count(client_id, coordinator_id, round_id)
        elif message_type == "COUNT_REPORT":
            _, client_id, sum_id, round_id, processed_packets = fields
            self._process_report(
                client_id, sum_id, round_id, processed_packets
            )
        elif message_type == "COMMIT":
            _, client_id = fields
            on_commit(client_id)
        else:
            raise ValueError(f"Unexpected sum control message: {fields}")
        ack()

    def start_consuming(self, on_message_callback):
        self.control_input_exchange.start_consuming(on_message_callback)

    def stop_consuming(self):
        """Solicita detener el consumidor desde el thread de su conexión.
        No hace nada si la conexión o el canal todavía no están disponibles.
        """

        connection_manager = self.control_input_exchange.conn
        connection = connection_manager.connection
        channel = connection_manager.channel
        if connection is None or not connection.is_open or channel is None:
            return
        connection.add_callback_threadsafe(
            self.control_input_exchange.stop_consuming
        )

    def close(self):
        exchanges = [
            self.control_output_exchange,
            self.control_start_exchange,
            self.control_input_exchange,
            *self.control_response_exchanges,
        ]
        for exchange in exchanges:
            exchange.close()