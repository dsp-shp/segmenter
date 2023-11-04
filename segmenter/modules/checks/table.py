from ..utils.table import Table
from sqlalchemy import engine
import pandas as pd
import typing

def check_table(
    con: typing.Union[engine.Connection, None],
    table_name: Table,
    get_data: bool = False,
    **kwargs
) -> typing.Tuple[pd.DataFrame, typing.Union[bool, None]]:
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
    params = {
        'sql': 'select * from {};'.format(table_name),
        'con': con
    }
    
    if get_data == True:
        data = pd.read_sql(**params)
    else:
        data = next(pd.read_sql(**params, chunksize=0))

    return (table_name.data, False if table_name.data is None else True)