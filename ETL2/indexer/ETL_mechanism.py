""" Техническое задание

В предыдущем модуле мы реализовывали механизм для для полнотекстового поиска.
Теперь улучшим его: научим его работать с новой схемой и оптимизируем количество элементов
для обновления.

Подсказки к выполнению задания по ETL

Перед тем как вы приступите к выполнению задания по теме ETL, мы дадим вам несколько полезных
подсказок:

1. Прежде чем выполнять задание, подумайте, сколько ETL-процессов вам нужно.
2. Для валидации конфига советуем использовать pydantic.
3. Для построения ETL-процесса используйте корутины.
4. Чтобы спокойно переживать падения Postgres или Elasticsearch, используйте решение с
техникой backoff или попробуйте использовать одноимённую библиотеку.
5. Ваше приложение должно уметь восстанавливать контекст и начинать читать с того места,
где оно закончило свою работу.
6. При конфигурировании ETL-процесса подумайте, какие параметры нужны для запуска приложения.
Старайтесь оставлять в коде как можно меньше «магических» значений.
7. Желательно, но необязательно сделать составление запросов в БД максимально обобщённым,
чтобы не пришлось постоянно дублировать код. При обобщении не забывайте о том, что все
передаваемые значения в запросах должны экранироваться.
8. Использование тайпингов поможет сократить время дебага и повысить понимание кода ревьюерами,
а значит работы будут проверяться быстрее :)
9. Обязательно пишите, что делают функции в коде.
10. Для логирования используйте модуль `logging` из стандартной библиотеки Python.

Желаем вам удачи в написании ETL. Вы обязательно справитесь

Решение задачи залейте в папку postgres_to_es вашего репозитория."""

import logging
from math import ceil

from etl_modules.etl_conrstants import ES_HOST
from etl_modules.etl_state import State

from etl_modules.extract_data import get_es_film_number, get_film_number, get_data
from etl_modules.establish_connection_es import connect_elasticrearch
from etl_modules.load_data import create_index, store_record, continue_from_state


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    es = connect_elasticrearch(ES_HOST)
    create_index(es)

    limit = 100
    film_number = get_film_number()
    film_number = film_number if film_number else 0
    bulk_number = ceil(film_number / limit)

    es_film_number = get_es_film_number(es) if get_es_film_number(es) else 0

    if es_film_number == 0:
        logging.info("Beginning data transfer")
        sender = store_record(bulk_number, es)
        sender.send(None)
        try:
            get_data(sender, bulk_number, limit)
        except StopIteration:
            logging.info("StopIteration in data transfer")
    elif film_number - es_film_number > limit:
        logging.info("Continuing data transfer")
        initial_state = State()
        continue_from_state(initial_state, bulk_number, limit, es)
    elif es_film_number == film_number:
        logging.info("Data were already transferred")

    final_state = State()
    final_state.clear_state()


