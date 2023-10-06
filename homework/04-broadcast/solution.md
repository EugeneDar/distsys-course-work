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

## Обоснование достижения требуемых свойств

### No Duplication

Дупликация сообщений не может произойти, так как мы отслеживаем доставленные сообщения.

### No Creation

Канал сам по себе надежный, и мы сами по себе также не создаем новых сообщений.

### Validity

Заметим, что сообщение будет доставлено, если выполнены два условия:
мы получили это сообщение хотя бы от половины процессов,
мы доставили необходимые для причинного порядка сообщения.

Если процесс корректен, то в конечном счёте он получит подтверждение больше чем от половины процессов,
так как отказать может только меньшинство.
Причинный порядок будет выполнен сразу, так как отправитель сам его и задаёт.

### Uniform Agreement

Если сообщение было доставлено некоторым процессом, значит оно получило его в последствии от большинства процессов.
Значит все корректные процессы получат это сообщение от большинства процессов.

Получаем: для того чтобы они его отправили им нужно до этого доставить все предыдущие по причинному порядку.
И мы видим, что для доставки этих предыдущих сообщений выполняются такие же правила. Поэтому по индукции 
мы видим, что в конце концов все сообщения будут доставлены.

Краткое пояснение индукции: будет сообщения, которому не нужно предыдущих, так как оно было первым,
его отправка разрешит отправку ненулевого числа других сообщений и так далее. Так мы и доходим до 
разрешения на отправку сообщения, которое рассматривали изначально.

### Causal Order

В начале рассылки каким-то процессов мы просто добавляем в рассылку информацию о всех сообщениях, которые
отправитель уже успел получить или доставить. И в дальнейшем доставляем сообщение другим процессом,
только если мы уже доставили все необходимые сообщения.

## Оптимизация

TODO