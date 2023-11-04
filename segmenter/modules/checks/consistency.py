from ..utils.table import Table
from datetime import datetime
from sqlalchemy import engine
import pandas as pd
import typing

def check_consistency(
    segment_id: str,
    con: engine.Connection,
    table_name: typing.Union[str, Table],
    **kwargs
) -> typing.Tuple[pd.Series, typing.Union[bool, None]]:
    """
    Проверка соответствия конфигурационному файлу
    =============================================

    Декорируется `logger.decorate(...)` методом в рамках работы Сегментера.

    Функция проверки согласованности между сегментной таблицей и конфигурационной
    segmenter/segmenter_segments.

    Возвращает:
        ...

    """
    data = pd.read_sql("""
        select 'test_segment' as segment_name
            , '{segment_id}' as segment_id
            , '{segment_id}' as segmenter_id
            , case when count(*) = 0 then false else true end as consistent
        from {table_name}
        where true
            and segment_id = '{segment_id}'
            and actual_end = 'infinity';
    """.format(segment_id=segment_id, table_name=table_name), con)

    return (data.iloc[0], data.iloc[0].consistent)