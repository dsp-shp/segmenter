from datetime import datetime
from sqlalchemy import engine
import pandas as pd
import typing

def check_relevance(
    segment_id: str = None,
    con: engine.Connection = None,
    **kwargs
) -> typing.Tuple[pd.core.frame.DataFrame, typing.Union[bool, None]]:
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
    """.format(segment_id=segment_id), con)

    return (data, data.iloc[0].relevance)