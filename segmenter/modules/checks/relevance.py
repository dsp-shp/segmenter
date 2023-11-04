from datetime import datetime
from sqlalchemy import engine
import pandas as pd
import typing

def check_relevance(
    con: engine.Connection,
    segment_id: typing.Union[str, None] = None,
    **kwargs
) -> typing.Tuple[pd.DataFrame, typing.Union[bool, None]]:
    """
    Проверка актуальности сегмента
    ==============================

    Декорируется `logger.decorate(...)` методом в рамках работы Сегментера.

    Возвращает:
        ...

    """
    data = pd.read_sql("""
        select case when count(*) = 1 then true else false end as relevance 
        from segmenter_log sl 
        where true 
            and id = '{segment_id}' 
            and "action" like 'refresh_%'
            and "action" != 'refresh_segments'
            and error is null
        ;
    """.format(segment_id=segment_id), con=con)

    return (data, data.iloc[0].relevance)