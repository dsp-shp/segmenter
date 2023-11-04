from collections import namedtuple
import pandas as pd
import typing

class Table(namedtuple('Table', ['schema', 'table'])):
    """
    Таблица
    =======

    Именованный кортеж, описывающий таблицу через свойства – атрибуты обращения 
    к ней: наименования схемы и таблицы.

    Методы:
    *    __new__: метод перегружает создание объекта с корректировкой для значений 
            вида 'таблица' без указания схемы – в таком случае свойству схемы 
            присваивается значение default_schema параметра
    *   __str__: метод перегружается для простоты обращения с классом, и
            возвращает форматированную строку вида: 'схема.таблица'

    """

    data: pd.DataFrame
    """ Данные, содержащиеся в таблице """

    schema: str
    """ Схема таблицы """

    name: str
    """ Наименование таблицы """
    
    @property
    def columns(self) -> typing.Union[list, None]: 
        """Перечень атрибутов коллекции"""
        return self.data.columns.tolist() if self.data else None
        
    def __new__(cls, table: str, default_schema: str = 'public'):
        return None if not table else super(Table, cls).__new__(
            cls,
            schema=table.split('.')[0] if '.' in table else default_schema,
            table=table.split('.')[-1]
        )
        
    def __init__(self, table: str, default_schema: str = 'public') -> None:
        """
        Инициализация объекта
        =====================

        Метод перегружает инициализацию объекта с получением набора атрибутов 
            коллекции вида: {атрибут1: тип данных, атрибут2: тип данных}

        Аргументы:
            table (str): схема и наименование таблицы в формате 'схема.таблица'
            default_schema (str): дефолтная схема хранилища
        
        """
        self.data = pd.DataFrame()

    def __str__(self) -> str:
        return '{}.{}'.format(self.schema, self.table)
    
