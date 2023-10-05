## Описание алгоритма

### Хранимые значения

Каждый процесс помимо своего id и id всех процессов хранит также некоторые
служебные поля, для выполнения всех необходимых гарантий.

Хранимые значения:
1. Счетчик числа полученных сообщений.
2. Сет уникальных id разосланных сообщений.
3. Сет уникальных id сообщений полученных от пользователей.
4. Сет уникальных id сообщений доставленных пользователю.
5. Словарь из уникальных id сообщений, которые мы хотим доставить, в два элемента: 
число процессов, от которых мы получили данное сообщение, и само сообщение.

### Метод on_local_message

Создаём уникальный id сообщения, рассылаем это сообщение всем кроме себя.
Добавляем его во все коллекции, кроме сета уникальных id доставленных сообщений.

### Метод try_deliver_messages

Проходим по списку всех сообщений, которые мы хотим отправить.
Проверяем, что мы имеем достаточное количество этого сообщения от других процессов.
Проверяем, что мы доставили все необходимые до этого сообщения.
Если проверки прошли, то доставляем сообщение, кладем его в сет доставленных и удаляем из списка желаемых.

### Метод on_message

Выходим, если уже отравляли это сообщение.

Обновляем количество этого сообщения от других процессов.
Если ранее мы это сообщение не рассылали, то выполняем рассылку.

Пытаемся доставить какое-то из сообщений, если смогли доставить что-то, то повторяем попытку.