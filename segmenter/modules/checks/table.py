from ..utils.table import Table
from sqlalchemy import engine
import pandas as pd
import typing

def check_table(
    table_name: typing.Union[str, Table],
    get_data: bool = False,
    con: engine.Connection = None,
    **kwargs
) -> typing.Tuple[pd.core.frame.DataFrame, typing.Union[bool, None]]:
    """
    Проверка доступности коллекции
    ==============================

    Декорируется `logger.decorate(...)` методом в рамках работы Сегментера.

    Аргументы:
        table (str): коллекция данных
        get_data (bool): необходимо ли возвращать записи или же достаточно лишь
            перечня атрибутов коллекции
        con (sqlalchemy.engine.Connection): активное подключение к хранилищу
    
    Возвращает:
        ...
    
    """
    get, chunksize = (lambda x:x, None) if get_data == True else (next, 0)
    table_name.data = get(
        pd.read_sql('select * from {};'.format(table_name), con, chunksize=chunksize)
    )
    return (table_name.data, False if table_name.data is None else True)