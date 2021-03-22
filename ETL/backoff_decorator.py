import time
from functools import wraps
import psycopg2


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое
    время, если возникла ошибка. Использует наивный экспоненциальный
    рост времени повтора (factor) до граничного времени ожидания
    (border_sleep_time)
    Формула:
    t = start_sleep_time * 2^(n) if t < border_sleep_time
    t = border_sleep_time if t >= border_sleep_time

    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            start_time = start_sleep_time
            border_time = border_sleep_time
            f = factor
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    start_time = (
                        start_time * (2 ** f)
                        if start_time * (2 ** f) < border_time
                        else border_time
                    )
                    print(
                        f"{str(e)} exception occurred, reconnecting in {start_time} seconds."
                    )
                    time.sleep(start_time)

        return inner

    return func_wrapper


@backoff()
def test_pg_connection():
    try:
        conn = psycopg2.connect(
            dbname="movies",
            user="postgres",
            password="12345",
            host="localhost",
            port=5432,
        )
    except Exception as e:
        raise Exception(str(e))


test_pg_connection()
