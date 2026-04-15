## Problema 1 Detectado y Estrategia de Solución

Durante el análisis de la implementación base se observó que el sistema cumplía correctamente con la división funcional entre sus componentes principales (Gateway, Sum, Aggregation y Join), pero presentaba limitaciones importantes al momento de operar en escenarios distribuidos con múltiples clientes concurrentes y varias réplicas de procesamiento.

El principal inconveniente detectado fue la ausencia de una identidad de consulta dentro del protocolo interno. Los mensajes intercambiados entre procesos contenían únicamente datos de negocio, como pares `(fruta, cantidad)` o estructuras parciales de resultados, pero no incluían información que permitiera asociarlos a un pedido específico. Esto implicaba que, ante la llegada simultánea de varios clientes, los registros podían mezclarse dentro de las estructuras internas de procesamiento, dificultando la correcta separación entre consultas y comprometiendo la validez de los resultados finales.

A su vez, el protocolo interno dependía de interpretaciones implícitas. El significado de cada mensaje se infería a partir de su forma o longitud, lo cual aumentaba el acoplamiento entre componentes y generaba fragilidad ante futuros cambios. Un mismo tipo de estructura podía representar datos, señales de finalización o resultados parciales según el contexto del receptor.

También se observó que algunos controles mantenían estado global compartido, lo cual impedía un procesamiento concurrente seguro. En este esquema, la finalización de una consulta podía interferir con otra aún en curso, ya que ambas utilizaban la misma memoria lógica para acumular resultados.

Con el objetivo de resolver estos problemas, se decidió rediseñar el protocolo interno adoptando un formato explícito y uniforme para toda la comunicación entre procesos. A partir de esta modificación, cada mensaje interno incluye cuatro elementos conceptuales: el tipo de mensaje, el identificador único de la consulta, el origen del mensaje y la carga útil correspondiente.

De esta manera, el sistema pasó a contar con una identidad de consulta (`query_id`) generada al inicio de cada pedido, lo que permite rastrear cada registro durante todo el pipeline distribuido. Gracias a esto, cada componente puede mantener estructuras independientes por consulta activa, evitando interferencias entre clientes concurrentes.

Además, se formalizaron distintos tipos lógicos de mensajes. Entre ellos se definieron mensajes de datos (`data`), mensajes de finalización (`eof`), resultados parciales (`partial_top`) y resultados finales (`final_top`). Esto elimina ambigüedades y permite que cada control procese eventos según un contrato claro y consistente.

Desde el punto de vista arquitectónico, esta decisión introduce una base mucho más robusta para escalar horizontalmente el sistema. La separación por consulta permite que múltiples instancias de procesamiento trabajen en paralelo sin mezclar información. Al mismo tiempo, el hecho de contar con mensajes tipados y trazables facilita la futura implementación de mecanismos de coordinación más avanzados, como conteo de finalizaciones parciales, sincronización entre réplicas y unión ordenada de resultados distribuidos.

En conclusión, la primera mejora realizada consistió en transformar un protocolo interno implícito y limitado en un mecanismo explícito, extensible y preparado para escenarios concurrentes. Esta decisión constituye la base estructural sobre la cual se apoyan las siguientes mejoras de coordinación y escalabilidad del sistema distribuido.