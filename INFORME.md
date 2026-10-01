# Coordinación y escalabilidad

## Coordinación de Sum y Aggregation

La coordinación entre las distintas instancias de Sum se realiza mediante un exchange de control interno. Cuando se conecta un cliente, el `message_handler` le asigna un `client_id` y numera consecutivamente sus paquetes de datos. El EOF incluye el siguiente número de paquete, por lo que restarle uno permite conocer cuántos paquetes de ese cliente se enviaron.

Cuando una instancia de Sum recibe el EOF original, actúa como coordinador y difunde una solicitud de conteo (`COUNT_REQUEST`) con el total esperado, que lo obtiene con el id del EOF que es el último mensaje para ese cliente. Cada réplica toma el lock para consultar cuántos paquetes de ese cliente procesó, guarda ese conteo para esa ronda de chequeo del coordinador y lo pone en cero; luego envía un `COUNT_REPORT` dirigido al coordinador. Si una solicitud repetida corresponde a la misma ronda, la réplica reutiliza el reporte guardado para no contar dos veces. El coordinador espera los reportes de todas las instancias y resta su suma del total pendiente. Si todavía quedan paquetes, inicia otra ronda de chequeo con la cantidad restante; cuando llega a cero, difunde `COMMIT`. Recién entonces cada Sum envía su acumulado al nodo de Aggregation correspondiente y notifica el EOF. El lock sincroniza el conteo y el procesamiento de mensajes de datos para que un paquete no quede fuera del reporte mientras se actualiza el estado.
El protocolo de conteo de mensajes procesados agrega mensajes de control y estado por cliente. En este TP se podria prescindir de él si se asume que, al propagar el EOF por el exchange, todas las instancias de Sum ya terminaron los mensajes anteriores. Sin embargo, no podemos garantizar eso debido a que el EOF viaja por un canal distinto y puede llegar mientras otras instancias aún procesan mensajes pendientes. Por eso, el chequeo de conteo de mensajes permite confirmar el cierre sin depender de esa suposición.

Al procesar el EOF, cada instancia de Sum envía sus datos al nodo de Aggregation correspondiente mediante un exchange. Una vez enviados, realiza un broadcast a todas las instancias de Aggregation para indicar que terminó de enviar los datos de ese cliente. El destino de cada mensaje se determina aplicando un hash a la fruta y al `client_id`. Así, las sumas parciales de una misma fruta para un mismo cliente se envían a la misma instancia de Aggregation.

En una primera implementación, el hash se aplicaba únicamente a la fruta. Sin embargo, esa decisión de diseño podía desaprovechar instancias de Aggregation, ya que si había menos tipos de frutas que nodos, algunos nodos no recibían datos. Incluir también el `client_id` en el hash permite distribuir mejor los pares cliente-fruta entre las instancias.

Por último, cada instancia de Aggregation reúne las sumas parciales de cada fruta y calcula un top parcial, que luego envía a Join. Join combina los tops parciales para obtener el top final y lo publica en la queue de salida, desde donde se informa al cliente.

## Escalabilidad y controles

El diseño permite escalar horizontalmente Sum y Aggregation, aunque el beneficio depende de la cantidad de clientes, registros y frutas distintas.

Las instancias de Sum consumen de una queue compartida, por lo que RabbitMQ reparte entre ellas los registros disponibles y agregar consumidores permite procesarlos en paralelo. Cada instancia acumula subtotales por cliente y fruta: así, cuando hay muchas frutas repetidas, el tráfico hacia Aggregation depende de las frutas distintas procesadas y no del total de filas ni clientes. Además, los acumulados ocupan memoria hasta el EOF, por lo que más clientes simultáneos o más frutas distintas aumentan el estado mantenido.

Las instancias de Aggregation reciben datos mediante un hash de `client_id` y fruta que dirige todos los subtotales de esa combinación a la misma instancia. Esto divide el trabajo en particiones independientes que pueden procesarse en paralelo. Sumar instancias ayuda cuando hay suficientes combinaciones para distribuir. Los nodos de Aggregation conservan las frutas distintas de cada cliente hasta recibir los EOF de todas los Sum, por lo que su memoria también crece con la cantidad de clientes y frutas distintas.

La escalabilidad con respecto a los clientes y los controles se basa en lo siguiente: el `client_id` separa el estado de cada conexión y permite procesar clientes distintos de manera concurrente; el costo es mantener estado temporal por cliente en las etapas. Para señalar el fin de los datos, cada una de las `S` instancias de Sum envía un EOF a las `A` instancias de Aggregation, por lo que son `S × A` mensajes de control por cliente, independientemente de la cantidad de filas. Por eso, reducir datos repetidos disminuye el tráfico de datos entre etapas, pero aumentar ambas cantidades de réplicas incrementa el costo de control. Join espera un top parcial de cada Aggregation y combina como máximo `A × TOP_SIZE` candidatos por cliente.


