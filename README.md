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

Al comenzar el trabajo, la implementación base resolvía correctamente los casos simples, pero al probar escenarios con varios clientes concurrentes y múltiples réplicas aparecieron varios problemas de coordinación, escalabilidad y cierre distribuido.

Lo primero que detecté fue que el sistema no tenía una forma clara de distinguir una consulta de otra dentro del protocolo interno. Los mensajes solo contenían datos de negocio, como fruta y cantidad, por lo que si dos clientes enviaban pedidos al mismo tiempo existía la posibilidad de mezclar información entre consultas distintas. Para resolver esto rediseñé el protocolo interno incorporando un `query_id` único para cada pedido y tipos de mensaje explícitos (`data`, `eof`, `partial_top`, `final_top`). De esta forma cada etapa del pipeline pudo mantener su estado separado por consulta y procesar múltiples clientes en paralelo sin interferencias.

Con ese cambio, el flujo quedó más claro. El `Gateway` recibe registros del cliente por TCP, genera una identidad lógica para la consulta y publica mensajes internos hacia la etapa `Sum`. Para eso utiliza RabbitMQ con una **cola de trabajo compartida** consumida por todas las instancias de `Sum`. Cada mensaje de datos contiene fruta, cantidad y el `query_id`, mientras que al finalizar la carga se envía un mensaje de cierre (`EOF`) para esa misma consulta.

La etapa `Sum` funciona como primer nivel de reducción distribuida. Cada instancia consume parte de los mensajes desde esa cola compartida, agrupa localmente por fruta y aplica la operación de suma provista por `FruitItem`. Esto permite que la mayor cantidad de trabajo ocurra en paralelo. En otras palabras, en vez de reenviar cada registro individual hacia adelante, cada `Sum` acumula localmente y luego transmite resultados ya reducidos.

Una vez resuelto eso, apareció un segundo problema entre `Sum` y `Aggregation`. En la versión original, cada instancia de `Sum` enviaba sus resultados a todas las instancias de `Aggregation`, es decir, se hacía broadcast de datos. Esto simplificaba la implementación inicial, pero generaba tráfico innecesario y además hacía que distintas réplicas repitieran trabajo sobre la misma información. Para mejorarlo implementé un particionado determinístico usando hashing por fruta. Así, cada fruta siempre queda asociada a una única instancia de `Aggregation`, evitando duplicación de cómputo y repartiendo mejor la carga entre nodos.

Para esto, cada `Sum` publica resultados en un **exchange** de RabbitMQ con varias claves de ruteo, una por instancia de `Aggregation`. Cada `Aggregation` consume únicamente la cola enlazada a su propia clave. De esa manera, todas las ocurrencias de una misma fruta llegan siempre al mismo nodo.

Con este esquema, si distintas instancias de `Sum` acumulan valores para `banana`, todas enviarán sus parciales a la misma instancia de `Aggregation`, que será la encargada de consolidar el total global de esa fruta. De esta forma `Aggregation` deja de repetir trabajo y pasa a actuar como segundo nivel de reducción distribuida, además de calcular luego un top parcial de su partición.

Después surgió el problema más delicado: el cierre distribuido de consultas. Cuando existen varias instancias de `Sum`, un único `EOF` publicado en una cola compartida solo es consumido por una réplica. Eso hacía que una instancia cerrara correctamente mientras las demás quedaban esperando o no emitían sus resultados.

La primera solución implementada fue un token de cierre que circulaba por la misma cola de entrada. Cuando una instancia de `Sum` consumía el `EOF` original, emitía sus acumulados, se añadía a una lista `visited_sum_ids` dentro del mensaje y lo reinsertaba en la cola para que la siguiente réplica lo tomara. El proceso continuaba hasta que todas las instancias habían participado. Esta estrategia preservaba el orden relativo con los datos y no requería coordinación externa, pero tenía dos sobrecostos concretos. Primero, el cierre era estrictamente secuencial: la latencia total escalaba linealmente con la cantidad de instancias de `Sum`, ya que cada réplica debía esperar el turno de la anterior. Segundo, cada reinserción del token implicaba un round-trip completo por el broker antes de que la siguiente instancia pudiera actuar, sumando latencia de red y de cola por cada `Sum` adicional.

Para eliminar ambos problemas se reemplazó esa estrategia por un mecanismo de colas de control individuales. La cola compartida de datos se mantiene igual; el `EOF` original sigue llegando por ahí y es consumido por una sola instancia. Esa instancia, en lugar de reinsertar el token, publica un aviso de cierre en paralelo a todas las colas de control, una por instancia de `Sum`. Cada instancia escucha su propia cola de control y, al recibir el aviso, cierra la consulta de forma independiente: emite sus acumulados hacia `Aggregation` y envía su `EOF` correspondiente. De esta manera el cierre ocurre en paralelo en todas las réplicas al mismo tiempo, con latencia constante independiente de cuántas instancias de `Sum` existan, y sin reinsertar ningún mensaje en la cola de datos.

También ajusté la sincronización de las últimas etapas. Cada instancia de `Aggregation` espera recibir la finalización de todas las instancias de `Sum` antes de calcular su `partial_top`. Recién en ese momento sabe que no llegarán más datos para esa consulta. Luego envía ese resultado parcial a `Join`.

Las instancias de `Aggregation` envían sus resultados a una **cola compartida** consumida por `Join`. Como cada `Aggregation` emite un único `partial_top` por consulta, el volumen transmitido en esta etapa es bajo.

La etapa `Join` concentra únicamente resultados ya resumidos. Su trabajo es esperar un `partial_top` de cada instancia de `Aggregation`, unirlos y calcular el top final global configurado. Como recibe pocos elementos por consulta (no todos los registros originales), su costo computacional es bajo comparado con las etapas anteriores, donde ocurre el procesamiento pesado.

Finalmente, el `Join` publica el resultado final en otra **cola de trabajo** consumida por el `Gateway`. El `Gateway` recibe ese mensaje, lo asocia mediante `query_id` con el cliente correcto y responde por TCP. Esto permite tener varias consultas concurrentes activas al mismo tiempo sin mezclar respuestas.
