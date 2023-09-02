from ..utils import Table
from sqlalchemy import engine
import pandas as pd
import typing

def refresh_procedure(
    table_name: Table,
    sql: str = None,
    con: engine.Connection = None,
    **kwargs
) -> typing.Tuple[pd.core.frame.DataFrame, typing.Union[bool, None]]:
    """
    Процедурный пересчет сегмента
    =============================

    Декорируется `logger.decorate(...)` методом в рамках работы Сегментера.

    Функция выполняет пересчет сегмента и возвращает результат работы.

    Аргументы:
        sql (str): вызов процедуры вида "call public.update_test_segment();"
        сon (sqlalchemy.engine.Connection): SQLalchemy подключение
        table_name (Table): сегментная таблица

    Возвращает:
        pd.core.frame.DataFrame: датафрейм с обработанными записями сегмента
    
    """
    con.execute(sql)

    return (pd.read_sql("""
        with _0 as (
            select distinct id, actual_begin, actual_end, processed
            from {}
            where true
                and processed = (select max(processed) from public.test_segment)
                and processed::date = now()::date
        )
        select distinct _0.id as processed, _1.id as added, _2.id as closed
        from _0
        left join _0 _1 on _0.id = _1.id and _1.actual_begin = _1.processed
        left join _0 _2 on _0.id = _2.id and _2.actual_end = _2.processed
        ;
    """.format(table_name), con),)