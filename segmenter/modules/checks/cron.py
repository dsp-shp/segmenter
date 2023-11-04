from cron_converter import Cron
from datetime import datetime
import pandas as pd
import typing

def check_cron(
    refresh_cron:str,
    date:datetime = datetime.now().replace(microsecond=0, tzinfo=None),
    **kwargs
) -> typing.Tuple[pd.Series, typing.Union[bool, None]]:
    """
    Проверка готовности по CRON-расписанию
    ======================================

    Декорируется `logger.decorate(...)` методом в рамках работы Сегментера.

    Возвращает:
        ...

    """
    cron_prev = Cron(refresh_cron).schedule().prev().replace(microsecond=0, tzinfo=None)
    cron_next = Cron(refresh_cron).schedule().next().replace(microsecond=0, tzinfo=None)

    return (pd.DataFrame([{
        'refresh_date': cron_prev,
        'refresh_cron': refresh_cron,
        'current_date': date,
        'refresh': cron_prev <= date < cron_next,
    }]).iloc[0], cron_prev <= date < cron_next)