from collections import namedtuple

class Table(namedtuple('Table', ['schema', 'table'])):
    """
    Таблица
    =======

    Именованный кортеж, описывающий таблицу через свойства – атрибуты обращения 
    к ней: наименования схемы и таблицы.

    Методы:
        __init__: метод перегружает инициализацию объекта с получением набора
            атрибутов коллекции вида: {атрибут1: тип данных, атрибут2: тип данных}
        __new__: метод перегружает создание объекта с корректировкой для значений 
            вида 'таблица' без указания схемы – в таком случае свойству схемы 
            присваивается значение default_schema параметра
        __str__: метод перегружается для простоты обращения с классом, и
            возвращает форматированную строку вида: 'схема.таблица'

    """
    def __new__(self, table: str, default_schema: str = 'public') -> object:
        return None if not table else super(Table, self).__new__(
            self,
            schema=table.split('.')[0] if '.' in table else default_schema,
            table=table.split('.')[-1]
        )
        
    def __init__(self, table: str, default_schema: str = 'public') -> None:
        self.data = None

    def __str__(self) -> str: 
        return '{}.{}'.format(self.schema, self.table)
    
    @property
    def columns(self) -> list: 
        return self.data.columns.tolist() if self.data else None