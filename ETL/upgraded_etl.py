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
import time
from elasticsearch import Elasticsearch

from etl_conrstants import ES_HOST, ES_INDEX
from etl_modules.backoff_decorator import backoff


def create_index(es_object, index_name="movies"):
    created = False
    index = ES_INDEX
    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=index)
            logging.info('Index created successfully.')
        else:
            logging.info("Index was already created.")
        created = True
    except:
        logging.error("Index wasn't created.", exc_info=True)
    finally:
        return created


@backoff()
def connect_elasticrearch(hostname: str):
    try:
        es_obj = Elasticsearch(hosts=[{"host": hostname}], retry_on_timeout=True)
        es_obj.cluster.health(wait_for_status="yellow")
        return es_obj
    except Exception as e:
        raise Exception(str(e))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    es = connect_elasticrearch(ES_HOST)
    create_index(es)
