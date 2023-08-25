from .table import Table
from io import StringIO
from re import findall
from traceback import format_exc
import inspect
import logging
import pandas as pd
import sys
import typing

class Logger(logging.Logger):
    """
    Система логирования
    ===================

    Методы:
        __init__: инициализация системы логирования
        __call__: реализация вызова метода класса
        __decorate_log (private): декоратор вывода сообщения – обертка поверх
            _log и дочерних методов info, warning, и т.д... для форматирования
            сообщения перед выводом – использовать только *args параметры
        _log: родительский метод вывода, декорируемый _decorate_log методом
        decorate: декоратор логирования
        getChild: реализация метода получения дочернего объекта
        getLevelName: получение наименования уровня логирования
    
    Свойства:
        id: свойство системы – идентификатор логирования
        table: свойство системы – логовая таблица в хранилище
        fmt: свойство системы – формат сообщения
    
    """
    def __init__(
        self, 
        id: str,
        table: str,
        name: str = 'Сегментер',
        level: int = logging.INFO,
        fmt: str = '[%(name)s] {%(levelname)s} – %(message)s',
        handler: logging.Handler = None, 
        connect: typing.Callable = None
    ) -> None:
        super().__init__(name, level)

        if not handler:
            self._handler = logging.StreamHandler(sys.stdout)
            self._handler.setFormatter(
                logging.Formatter(fmt=fmt)
            )
        else:
            self._handler = handler
        self.addHandler(self._handler)

        self._id = id
        self._table = Table(table)
        self._fmt = fmt
        
        self.info(
            'Инициализировано логирование для {}\n' + '-' * 69, self._id
        )
        
        if not connect:
            self.warning('Не передан метод подключения к хранилищу')
            return
        
        self.connect = connect
        with self.connect() as con:
            self._table.data = pd.read_sql_query(
                'select * from {} limit 0;'.format(self._table), 
                con
            )

    def __call__(self, method: typing.Callable) -> typing.Callable:
        return dict(inspect.getmembers(self)).get(method, None)

    @property
    def id(self) -> str: return self._id

    @property
    def table(self) -> Table: return self._table

    @property
    def fmt(self) -> str: return self._fmt

    def __decorate_log(func: typing.Callable) -> typing.Callable:
        """
        Декоратор вывода сообщения
        ==========================

        """
        def wrapper(self, level: int, msg: str, args: list, **kwargs) -> None:
            offset = ' ' * len(
                self.fmt % {
                    'name': self.name, 
                    'levelname': self.getLevelName(), 
                    'message':''
                }
            )
            # if not kwargs.get('repeat'):
            #     self.info(str(locals()).replace('{', '{{').replace('}', '}}'), repeat=True)

            fmt_arg = len(findall('{}', msg))
            func(
                self,
                level, 
                msg.format(*args[:fmt_arg], **kwargs).replace('\n', '\n' + offset),
                args[fmt_arg:],
                exc_info=kwargs.get('exc_info', None), 
                extra=kwargs.get('extra', None),
                stack_info=kwargs.get('stack_info', False),
                stacklevel=kwargs.get('stacklevel', 1)
            )
        return wrapper

    @__decorate_log
    def _log(self, level: int, msg: str, *args, **kwargs) -> None:
        super()._log(level, msg, *args, **kwargs)

    def getChild(self, suffix: str) -> logging.Logger:
        return self.__class__(
            self._id, 
            self._table,
            self.name + '.' + suffix,
            self.level,
            self._fmt,
            self._handler,
            self.connect
        )
    
    def getLevelName(self) -> str:
        return logging.getLevelName(self.level)

    def decorate(self, func: typing.Callable, *args, **kwargs) -> typing.Callable:
        """
        Декоратор логирования
        =====================

        Декоратор формирует логовую запись параметрами функции, передаваемыми при
        вызове декорируемой функции (во избежание конфликтов при вставке записи 
        в логовую коллекцию первая принимает только те атрибуты, что содержатся 
        в последней), вызывает декорируемую функцию, дополняет запись результатом 
        ее выполнения. Далее (через переданный функции логгер) логирует сообщение 
        – в sys.stdout и запись – в логовую коллекцию, после чего возвращает 
        результат выполнения или None – в случае ошибки.

        """
        def wrapper(
            *args,
            func: typing.Callable = func,
            logger: logging.Logger = self, 
            **kwargs
        ) -> typing.Any:

            def _format_res(data: typing.Any) -> str:
                """
                Форматирование результата
                =========================

                Для датафрейма: вернуть info() строку, читаемую в StringIO буфер
                    строчки датафрейма: вернуть to_string() строку
                    любого другого типа: вернуть str() строку

                """
                if isinstance(data, pd.core.frame.DataFrame):
                    with StringIO() as buf:
                        data.info(buf=buf)
                        return buf.getvalue()
                elif isinstance(data, pd.core.series.Series):
                    return '\n'.join((str(type(data)), *data.to_string().splitlines(), ''))
                else:
                    return str(data)

            try:
                __flat_locals = {**locals(), **locals().get('kwargs', {})}
                __signature = {**inspect.signature(func).parameters}
                __signature_kwargs = {
                    k: __flat_locals.get(k, None) for k,v in __signature.items() if True \
                        and not v.default is inspect._empty
                }
                __signature_kwargs = {k:(v if v else __signature[k].default) for k,v in __signature_kwargs.items()}
                __signature_args = {_: __flat_locals.get(_, None) for _ in [
                    k for k,v in __signature.items() if True \
                        and v.default is inspect._empty \
                        and k not in ('kwargs',)
                        and not k.startswith('__')
                ]}
                for i in __flat_locals['args']:
                    for j in __signature_args:
                        if __signature_args[j]:
                            continue
                        else:
                            __signature_args[j] = i
                params = {_[0]: str(_[1]) for _ in (*__signature_args.items(), *__signature_kwargs.items())}
            except Exception:
                logger.warning(format_exc())

            message = findall('[^\ \n]+.+[^\ \n]+', func.__doc__)[0]
            log = {
                **{k:v for k,v in kwargs.items() if k in logger.table.data.columns},
                'id': str(logger.id),
                'action': func.__name__,
                'params': str(params),
                'message': message,
                'error': None
            }

            try:
                with logger.connect() as con:
                    data, *__data = func(*args, con=con, **kwargs)
                message += ': ' + _format_res(data)
                log.update({'message': message})
                logger.info(message)
            except Exception:
                data = None
                message += ':\n' + format_exc()
                log.update({'error':format_exc()})
                logger.error(message)
            finally:
                if logger.connect:
                    with logger.connect() as con:
                        pd.DataFrame((log,)).to_sql(
                            name=logger.table.table,
                            con=con, 
                            schema=logger.table.schema,
                            if_exists='append',
                            index=False
                        )
                return __data[0] if __data else data

        return wrapper