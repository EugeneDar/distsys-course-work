# Распределенное key-value хранилище с репликацией

В этой задаче вам предстоит усовершенствовать key-value хранилище из предыдущего ДЗ, обеспечив отказоустойчивое хранение данных путем их репликации. Теперь каждая запись должна храниться не на одном узле, а на трех. Таким образом, при выходе из строя 1-2 узлов, данные не должны теряться. При этом остается шардинг данных, то есть в системе из 6 узлов каждый будет хранить примерно половину данных. Чтобы не создавать зависимость от предыдущего ДЗ, будем использовать простейший (и не самый эффективный) способ шардинга данных и обойдемся без перебалансировки шардов. Тем самым, сконцентрируемся теперь на реализации репликации данных и связанных с этим проблемах. 

Пусть в нашей системе есть _N_ узлов с идентификаторами _[0, N)_. Тогда за хранение ключа _K_ будет отвечать узел с идентификатором _P_ = _hash(K) mod N_. А, так как мы хотим реплицировать данные на трех узлах, то добавим к этому узлу еще два узла, следующие за ним в порядке их идентификаторов. Например, если _N_=6 и для некоторого ключа _P_=4, то за хранение этого ключа будут отвечать узлы _{4, 5, 0}_. Далее будем называть эти узлы и хранимые ими копии данных _репликами_. 

Для реализации репликации будем использовать подход без лидера, описанный в [статье про Amazon Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf). Операции на чтение (GET) и запись (PUT, DELETE) выполняются с _кворумами_, размеры которых указывает клиент при отправке операции. Запрос на выполнение операции может быть сделан локально с любого узла. Узел рассылает операцию по репликам и, при получении кворума ответов, сообщает результат операции локальному клиенту.

Мы хотим сделать наше хранилище высокодоступным и уметь успешно обрабатывать запросы в присутствии отказов узлов и сети. Если из-за отказов для успешного выполнения операции недостаточно живых реплик (не набирается нужный кворум), то в качестве резервных реплик должны использоваться другие доступные узлы, в порядке их следования за основными репликами. Например, если основные реплики ключа это узлы _{2, 3, 4}_, то при отказе одного из них в качестве резервной реплики должен использоваться узел _5_. Если отказавшая реплика стала снова доступна, то резервная должна передать ей записанные данные. Этот подход (sloppy quorum + hinted handoff) также описан в статье про Dynamo.

Узлы и сеть могут отказывать во время обработки запросов. Обнаруживать отказы можно с помощью таймаутов. Для простоты будем считать, что отказы временные, и в случае обнаружения отказа не требуется делать перебалансировку шардов или переназначать реплики между узлами. Кроме того, вашей реализации не требуется поддерживать добавление и удаление узлов из системы, в тестах состав узлов фиксирован.

В силу описанного подхода, операции записи в нашей системе могут возвращать клиенту успех до того, как изменения достигнут всех реплик, а чтение может возвращать устаревшее значение. При этом система должна обеспечивать _согласованность в конечном счёте_ (eventual consistency) - изменения должны рано или поздно оказаться на всех живых репликах. Для этого надо как минимум реализовать механизмы _hinted handoff_ и _read repair_. Реализация фоновой синхронизации (anti-entropy) - по желанию.

Из-за отказов и конкурентных операций записи на разных узлах системы может оказаться несколько версий значения ключа. Ваша реализация должна уметь автоматически разрешать возникающие конфликты при записи и чтении, используя стратегию _last write wins (LWW)_. А именно, с каждой операцией записи и соответствующим значением ключа связано время записи, и из двух значений выбирается то, которое было записано позднее. Если же времена совпадают, то выигрывает большее значение (в лексикографическом порядке). Клиенту всегда возвращается только одно значение ключа. Данная стратегия подразумевает синхронизацию часов между узлами. В тестах это предположение выполняется, поэтому в своей реализации вы можете опираться на время, доступное через `ctx.time()`. Другим недостатком данной стратегии является возможность потери изменений, сделанных клиентами. Мы устраним эти недостатки в следующем задании (**Внимание! Следующее ДЗ будет требовать наличия готового решения текущей задачи**).

Если вам плохо понятны некоторые требования, изучите соответствующие тесты - это часть условия задачи.

## Реализация

Для реализации и тестирования решения используется учебный фреймворк dslab-mp (см. материалы первого семинара). В папке задачи размещена заготовка для решения [solution.py](solution.py). Вам надо доработать реализацию узла в классе `StorageNode` так, чтобы проходили все тесты.

При инициализации узлу передается его уникальный id, а также список id всех узлов в системе.

Узел должен поддерживать обработку следующих локальных сообщений (форматы запросов и ответов описаны в заготовке):
- _GET(key, quorum)_ - вернуть значение записи с ключом `key` (может выдать пустое значение, если записи с этим ключом нет),
- _PUT(key, value, quorum)_ - сохранить запись с ключом `key` и значением `value` (в ответе должны возвращаться сохраненные ключ и значение, второе может отличаться от `value` в случае конкурентных запросов),
- _DELETE(key, quorum)_ - удалить запись с ключом `key` и вернуть её последнее значение (ответ должен быть эквивалентен ответу на _GET_ с тем же ключом, то есть по сути удаление совмещено с кворумным чтением).

В отличие от предыдущего ДЗ, все операции с хранилищем теперь указывают используемый размер кворума.

Для вычисления реплик по ключу используйте функцию `get_key_replicas()` из заготовки (использовать другую функцию не следует, так как это влияет на тесты).

Для взаимодействия между узлами вы можете использовать любые собственные типы сообщений.

## Тестирование

Перед запуском тестов убедитесь, что на вашей машине [установлен Rust](https://www.rust-lang.org/tools/install) (версия не ниже 1.62).

Тесты находятся в папке `tests`. Для запуска тестов перейдите в эту папку и выполните команду: `cargo run --release`. Запустить только один из тестов можно с помощью опции `-t`. По умолчанию вывод тестов не содержит трассы (последовательности событий во время выполнения каждого из тестов), а только финальную сводку. Включить вывод трасс можно с помощью флага `-d`. Все доступные опции можно посмотреть с помощью `cargo run --release -- --help`.

Если вы найдете ошибки или требования из условий, которые не покрывают наши тесты, то вы можете получить за это бонусы. Для этого надо включить в отчёт описание ситуации, которую не ловят тесты, добавив при необходимости пример решения с ошибкой. За это полагается 1 балл. Если вы также реализуете тесты, которые ловят найденную проблему, или хотя бы опишите их логику, то получите еще 1 балл. Готовые тесты оформляйте как pull request в репозиторий курса.

## Оценивание

Компоненты задачи и их вклад в оценку:
- Отчёт с описанием вашего решения в файле `solution.md` - обязательно, без него проверка производиться не будет.
- Базовый функционал (тесты BASIC, REPLICAS CHECK, MC BASIC) - 3 балла.
- Разрешение конфликтов при записи (тесты ...CONCURRENT WRITES...) и чтении (тесты STALE REPLICA..., DIVERGED REPLICAS) - 3 балла.
- Поддержка sloppy-кворумов и разделения сети (тесты ...SLOPPY QUORUM... и PARTITION...) - 3 балла.
- Описание любой ситуации, когда при кворумах _W_ + _R_ > _N_ система может выдать не последнее успешно записанное значение (запись успешна, если клиент получил _W_ ответов) - 1 балл.

## Сдача

Следуйте стандартному [порядку сдачи заданий](../readme.md).

## Рекомендации

**Начните решать задачу за несколько вечеров до дедлайна, так как можно неожиданно для себя оставить целый вечер на дебаге чего-то, что вы неправильно поняли.**

Мы реализовали полное решение и предлагаем следующий план как решать задачу:

1. Изучите [статью про Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) (фактически в этом и следующем задании мы реализуем некоторое её подмножество). Далее будет полезно возвращаться к статье по мере реализации отдельных пунктов.
2. Реализуйте базовую версию репликации с классическими кворумами.

> Не забывайте про дебаг с помощью `cargo run --release -- -t 'TEST NAME' -d`.

3. Реализуйте разрешение конфликтов на базе LWW для случаев, когда реплике приходит запрос на запись более "старого" значения, чем хранимое ей, и когда при чтении узлу приходят ответы с разными значениями от реплик.

> Поскольку взаимодействие между узлами происходит в формате запрос-ответ, лучше сразу сделать какой-то контекст запроса и его передавать, а если очень хочется решить архитектурную задачу — можно написать маленький фреймворк для коммуникации в формате запрос-ответ (с таймаутами, они понадобятся для sloppy quorum) поверх базового узла из заготовки.

4. Реализуйте _read repair_.

> Имеет смысл дожидаться ответов от всех реплик, даже если мы уже ответили клиенту, чтобы "починить" отставшие (см. главу 5 из статьи).

5. Реализуйте _sloppy quorum_.

> Удобнее всего это сделать с помощью таймаутов. Сначала узел пытается отправить запрос на основные реплики, и если в течение таймаута кворум не набран, обращается к резервным.

6. Реализуйте _hinted handoff_.
