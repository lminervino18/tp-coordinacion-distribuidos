# Trabajo Práctico - Coordinación

En este trabajo se busca familiarizar a los estudiantes con los desafíos de la coordinación del trabajo y el control de la complejidad en sistemas distribuidos. Para tal fin se provee un esqueleto de un sistema de control de stock de una verdulería y un conjunto de escenarios de creciente grado de complejidad y distribución que demandarán mayor sofisticación en la comunicación de las partes involucradas.

## Ejecución

`make up` : Inicia los contenedores del sistema y comienza a seguir los logs de todos ellos en un solo flujo de salida.

`make down`:   Detiene los contenedores y libera los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make test`: Inicia los contenedores del sistema, espera a que los clientes finalicen, compara los resultados con una ejecución serial y detiene los contenederes.

`make switch`: Permite alternar rápidamente entre los archivos de docker compose de los distintos escenarios provistos.

## Elementos del sistema objetivo

![ ](./imgs/diagrama_de_robustez.jpg  "Diagrama de Robustez")
*Fig. 1: Diagrama de Robustez*

### Client

Lee un archivo de entrada y envía por TCP/IP pares (fruta, cantidad) al sistema.
Cuando finaliza el envío de datos, aguarda un top de pares (fruta, cantidad) y vuelca el resultado en un archivo de salida csv.
El criterio y tamaño del top dependen de la configuración del sistema. Por defecto se trata de un top 3 de frutas de acuerdo a la cantidad total almacenada.

### Gateway

Es el punto de entrada y salida del sistema. Intercambia mensajes con los clientes y las colas internas utilizando distintos protocolos.

### Sum
 
Recibe pares  (fruta, cantidad) y aplica la función Suma de la clase `FruitItem`. Por defecto esa suma es la canónica para los números enteros, ej:

`("manzana", 5) + ("manzana", 8) = ("manzana", 13)`

Pero su implementación podría modificarse.
Cuando se detecta el final de la ingesta de datos envía los pares (fruta, cantidad) totales a los Aggregators.

### Aggregator

Consolida los datos de las distintas instancias de Sum.
Cuando se detecta el final de la ingesta, se calcula un top parcial y se envía esa información al Joiner.

### Joiner

Recibe tops parciales de las instancias del Aggregator.
Cuando se detecta el final de la ingesta, se envía el top final hacia el gateway para ser entregado al cliente.

## Limitaciones del esqueleto provisto

La implementación base respeta la división de responsabilidades de los distintos controles y hace uso de la clase `FruitItem` como un elemento opaco, sin asumir la implementación de las funciones de Suma y Comparación.

No obstante, esta implementación no cubre los objetivos buscados tal y como es presentada. Entre sus falencias puede destactarse que:

 - No se implementa la interfaz del middleware. 
 - No se dividen los flujos de datos de los clientes más allá del Gateway, por lo que no se es capaz de resolver múltiples consultas concurrentemente.
 - No se implementan mecanismos de sincronización que permitan escalar los controles Sum y Aggregator. En particular:
   - Las instancias de Sum se dividen el trabajo, pero solo una de ellas recibe la notificación de finalización en la ingesta de datos.
   - Las instancias de Sum realizan _broadcast_ a todas las instancias de Aggregator, en lugar de agrupar los datos por algún criterio y evitar procesamiento redundante.
  - No se maneja la señal SIGTERM, con la salvedad de los clientes y el Gateway.

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y realizar una implementación que funcione correctamente ante cambios en la multiplicidad de los controles (archivo de docker compose), los archivos de entrada y las implementaciones de las funciones de Suma y Comparación del `FruitItem`.

![ ](./imgs/mutabilidad.jpg  "Mutabilidad de Elementos")
*Fig. 2: Elementos mutables e inmutables*

A modo de referencia, en la *Figura 2* se marcan en tonos oscuros los elementos que los estudiantes no deben alterar y en tonos claros aquellos sobre los que tienen libertad de decisión.
Al momento de la evaluación y ejecución de las pruebas se **descartarán** o **reemplazarán** :

- Los archivos de entrada de la carpeta `datasets`.
- El archivo docker compose principal y los de la carpeta `scenarios`.
- Todos los archivos Dockerfile.
- Todo el código del cliente.
- Todo el código del gateway, salvo `message_handler`.
- La implementación del protocolo de comunicación externo y `FruitItem`.

Redactar un breve informe explicando el modo en que se coordinan las instancias de Sum y Aggregation, así como el modo en el que el sistema escala respecto a los clientes y a la cantidad de controles.

---

## Informe de Coordinación y Escalabilidad

### Protocolo interno y concurrencia de clientes

La implementación base no distinguía una consulta de otra: los mensajes solo portaban fruta y cantidad, por lo que dos clientes concurrentes podían mezclar información. La solución fue incorporar un `query_id` único por consulta al protocolo interno, junto con tipos de mensaje explícitos (`data`, `eof`, `partial_top`, `final_top`). Cada etapa mantiene su estado particionado por `query_id`, lo que permite procesar múltiples clientes en paralelo sin interferencias.

### Sum: primer nivel de reducción

El `Gateway` publica mensajes en una **cola de trabajo compartida** consumida por todas las instancias de `Sum`. Cada instancia acumula localmente los registros que le llegan, aplicando la operación `FruitItem.suma` por fruta. Al cerrar la consulta emite sus parciales ya reducidos, no los registros individuales. Agregar instancias de `Sum` divide el trabajo de acumulación linealmente.

### Particionado Sum → Aggregation

En la versión original cada `Sum` hacía broadcast a todas las `Aggregation`, lo que implicaba trabajo redundante. Se reemplazó por particionado determinístico: cada `Sum` calcula `CRC32(fruta) % N` para decidir a qué `Aggregation` enviar cada fruta. Para esto publica en un **direct exchange** con una routing key por instancia de `Aggregation` (`aggregation_prefix_i`); cada instancia declara y bindea únicamente su propia cola a ese exchange. Así, todos los parciales de una misma fruta —provenientes de distintos `Sum`— convergen siempre en la misma `Aggregation`.

### Cierre distribuido de consultas

Este fue el problema más delicado. El `EOF` que publica el `Gateway` en la cola compartida es consumido por **una sola** instancia de `Sum`; las demás no reciben ninguna señal de cierre.

**Primera solución (descartada): token circulante.** La instancia que consumía el `EOF` emitía sus acumulados, agregaba su ID a una lista `visited` y reinsertaba el token en la misma cola. Correcto en términos de orden, pero el cierre era estrictamente secuencial: la latencia total escalaba linealmente con N, sumando un round-trip de broker por cada instancia.

**Segunda solución (descartada): colas de control individuales.** La instancia que consumía el `EOF` publicaba un aviso en paralelo a N colas dedicadas, una por `Sum`. Cada instancia escuchaba su propia cola de control en el mismo canal que la cola de datos, y al recibir el aviso emitía sus resultados de forma independiente. El cierre ocurría en paralelo con latencia constante. Sin embargo, el aviso viajaba por un canal distinto al de los datos: aunque ambos consumers compartían la misma conexión TCP, el broker podía intercalar tramas de canales distintos, introduciendo una race-condition donde el aviso de cierre llegara antes de que `Sum_Y` terminara de procesar mensajes de datos ya pre-entregados. La solución se mitigaba con el `prefetch_count` y descartando mensajes tardíos, pero la race no podía eliminarse por construcción.

**Solución final: coordinación con validación de conteo.** Se elimina la race-condition de forma definitiva con un protocolo de tres fases:

1. **Fan-out.** El `Gateway` incluye en su `EOF` el total de mensajes de datos enviados para esa consulta (`total_sent`). La instancia de `Sum` que consume ese `EOF` actúa como iniciadora: publica un `close_signal` con ese total a un **fanout exchange** (`sum_prefix_close_exchange`), lo que hace llegar el mensaje a todos los `Sum` con una sola publicación. Cada instancia tiene una cola de control dedicada (`sum_prefix_close_{id}`) bindeada a ese exchange y registrada como consumer extra en el **mismo canal** que la cola de datos.

2. **Fan-in.** Al recibir el `close_signal`, cada `Sum` —incluyendo la iniciadora— responde con su conteo actual de mensajes procesados para esa consulta, enviándolo a través de un **direct exchange** (`sum_prefix_count_exchange`) con routing key `sum_prefix_count_{initiator_id}`, es decir, directo a la cola del iniciador. Mientras tanto continúa consumiendo datos normalmente: si aún quedan mensajes pendientes en la cola compartida los procesa y actualiza su conteo. Si la suma de todos los conteos no alcanza `total_sent`, la iniciadora emite un `recheck_signal` y espera una nueva ronda de respuestas. Este loop converge porque se puede asumir que ningún mensaje se pierde y los `Sum` siguen procesando datos entre rondas.

3. **Commit.** Cuando la suma acumulada iguala `total_sent`, la iniciadora emite un `go_signal` al mismo **fanout exchange** de cierre. Cada instancia, al recibirlo, emite sus resultados parciales al exchange de `Aggregation` y su `EOF` correspondiente.

**Trade-offs.** La validación por conteo añade latencia de coordinación: al menos dos round-trips por consulta (fan-out + fan-in + fan-out), contra el único round-trip de la solución anterior. En escenarios de alto throughput con muchos Sums el protocolo puede requerir varias rondas de recheck si el prefetch genera mensajes en vuelo en el momento del cierre, aunque en la práctica converge en la primera ronda. A cambio, la corrección es garantizada por construcción: si `total_sent` es exacto y no hay pérdida de mensajes, es imposible cerrar una consulta con datos faltantes. Cada instancia de `Sum` abre exactamente **4 conexiones AMQP** independientemente de cuántos `Sum` o `Aggregation` haya: la cola de entrada, el fanout exchange de cierre, el direct exchange de conteos y el direct exchange de salida.

### Sincronización en Aggregation y cierre hacia Join

Cada `Aggregation` recibe `EOF` de cada instancia de `Sum`. Mantiene un conjunto de IDs de `Sum` que ya cerraron y espera hasta tener confirmación de todos (`len(completed_sums) == SUM_AMOUNT`) antes de calcular su `partial_top`. Esa barrera garantiza que no se emita ningún resultado parcial mientras aún pueden llegar datos de alguna instancia de `Sum`. Una vez completa la barrera, emite el top-K de su partición a una **cola compartida** consumida por `Join`.

### Join y respuesta al cliente

`Join` espera exactamente un `partial_top` de cada `Aggregation`. Cuando los recibe todos, los fusiona aplicando nuevamente `FruitItem.suma` para consolidar frutas que aparecen en más de una partición, calcula el top-K global y publica el resultado final. El `Gateway` recibe ese mensaje, lo asocia al cliente correcto por `query_id` y responde por TCP.
