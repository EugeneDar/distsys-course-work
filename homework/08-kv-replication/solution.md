# Описание решения

## Обработка запросов

При каждом входящем локальном запросе мы создаем локальную структуру для поддержания текущей информации о состоянии кворума.
Далее мы отправляем сообщения во все реплики. В свою очередь реплики получив такое сообщение отвечают.
Каждый раз, когда приходит результат от реплик, мы добавляем его локально и проверяем, если набрали кворум.
Если набрали, то отправляем ответ пользователю.

## Разрешение конфликтов

Для разрешения конфликтов чтения и записи с каждым запросом также идёт время. 
В случае конфликтов выбирается максимальное время, а при его равенстве -
максимальное значение.

## Read repair

Так как до какой-то из реплик по причине проблем сети или отказов могли не дойти актуальные значения, 
то для каждого запроса, собирая ответы для кворума (а также после того как собрали его) мы отправляем сообщение `REFRESH` для обновления данных реплики.

## Sloppy quorum

Начав собирать кворум процесс ставит таймер, за время работы которого необходимо данный кворум собрать. 
По срабатыванию таймера мы проверяем, что все реплики ответили. 
Если кто-то не ответил, то мы замещаем их другими узлами и отравляем запрос им.

Важно, что мы проверяем не факт того, что кворум собрался, а факт ответа всеми репликами.

## Hinted handoff

Если реальные реплики по какой-то причине были замещены другими узлами, а потом вновь стали доступны, то мы хотим, чтобы они получили актуальные значения.
Для этого замещающие узлы для всех запросов, которые они получили в качестве заместителей, создают список.
Пока этот список не пустой, постоянно ставится таймер, который рассылает эти запросы реальным репликам, и когда приходит ответ от реальных реплик, то запрос удаляется из этого списка.

# Ситуации, когда может выдать не последнее записанное значение

## Пример 1

Пусть у нас `W` = 1, `R` = 3. 

Мы выполняем `PUT`, он доходит до первой реплики и обрабатывается там, она возвращает нам ответ, мы отдаем его клиенту. Происходит отказ первой реплики. Выполняется `GET`, так как у нас отказ, то
используется вспомогательная вершина. Пока что никто из трёх вершин (вспомогательной, второй и третьей реплик) не успел записать никакие данные. Поэтому они возвращают пустой результат.
И только в этот момент сообщения от самого первого `PUT` доходят до второй и третьей реплик.

## Пример 2

Пусть у нас `W` = 3, `R` = 1.

Первая реплика отказывает. Выполняется `PUT`, его результат записывается во вторую и третью реплики, а также во вспомогательную вершину. 
Приходит запрос `GET`. Первая реплика оживает и первой отвечает на запрос. Пользователю возвращается пустое значение.
