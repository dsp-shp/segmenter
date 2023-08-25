from ..utils import Table
from sqlalchemy import engine
import pandas as pd
import typing

def refresh_query(
    table_name: Table,
    sql: str = None,
    con: engine.Connection = None,
    **kwargs
) -> typing.Tuple[pd.core.frame.DataFrame, typing.Union[bool, None]]:
    """
    Пересчет сегмента по запросу
    ============================

    Декорируется `logger.decorate(...)` методом в рамках работы Сегментера.

    Функция выполняет пересчет сегмента и возвращает результат работы.

    Аргументы:
        sql (str): произвольный SQL запрос на получение данных
        сon (sqlalchemy.engine.Connection): SQLalchemy подключение
        table_name (Table): сегментная таблица

    Возвращает:
        pd.core.frame.DataaFrame: датафрейм с обработанными записями сегмента
    
    """
    columns = pd.read_sql_query(sql, con=con).columns
    _columns = ['_.' + x for x in columns if x != 'id']
    _new_columns = {('_new.' + x): ('new_' + x) for x in columns if x != 'id'}
    
    con.execute(
        """                
            drop table if exists _new;
            create temp table _new as
            {};
        """.format(sql) + \
        """ 
            /*
                Построить сетку для соответствия между уже имеющимися в сегменте
                данными и новыми записями.
            */
            drop table if exists _grid;
            create temp table _grid as
            select coalesce(_.id, _new.id) as id
                , {columns}
                , {new_columns_as}
                , case
                    when _.id is null then false
                    else true
                end as existed
                , case
                    when ({columns}) = ({new_columns}) then false
                    else true
                end as changed
            from {table_name} _
            right join _new on true 
                and _.id = _new.id 
                and _.actual_end = 'infinity';
        """.format(
            columns = ', '.join(_columns),
            new_columns = ', '.join(_new_columns.keys()),
            new_columns_as = ', '.join(map(lambda x: x[0] + ' as ' + x[1], _new_columns.items())),
            table_name = table_name
        ) + \
        """ 
            /*
                Вставить в сегмент новые записи.
            */
            insert into {table_name} ({columns})
            select id, {new_columns}
            from _grid
            where not existed;
        """.format(
            table_name = table_name,
            columns = ', '.join(columns),
            new_columns = ', '.join(_new_columns.values())
        ) + \
        """ 
            /*
                Обновить "processed" атрибут в сегменте для актуальных и уже 
                существующих в сегменте записей.
            */
            update {table_name} 
            set processed = now() 
            from _grid 
            where true 
                and test_segment.id = _grid.id 
                and test_segment.actual_end = 'infinity' 
                and _grid.existed;
        """.format(table_name = table_name) + \
        """ 
            /*
                Закрыть версии для измененных, уже существующих, актуальных в 
                этом сегменте записей.
            */
            update {table_name}
            set actual_end = now() 
            from _grid 
            where true 
                and test_segment.id = _grid.id 
                and test_segment.actual_end = 'infinity' 
                and _grid.changed;
        """.format(table_name = table_name)
    )

    return (pd.read_sql_query("""
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