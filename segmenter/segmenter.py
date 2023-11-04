from .modules import checks, refreshers, updaters
from .modules.utils import Logger, Table
from contextlib import contextmanager
from datetime import datetime
from sqlalchemy import create_engine, engine
from uuid import uuid4
from types import SimpleNamespace
import pandas as pd
import inspect
import typing

class Segmenter:
    """
    Сегментер
    =========

    Класс, реализующий управление сегментами и аудиториями.

    Методы:
    *   __init__: инициализация объекта, сервисных апдейтеров и логирования,
            подключение к хранилищу данных, проверка доступности коллекций
    *   connect (contextmanager): подключение к хранилищу данных
    *   select_segments: получение актуальных сегментов
    *   refresh_segments: batch-пересчет сегментов
    *   select_audiences: получение перечня аудиторий (из кабинета)
    *   update_audiences: обновление аудиторий

    """
    def __init__(
        self, 
        con: dict,
        cfg_table: str = 'segmenter',
        log_table: str = 'segmenter_log',
        consumers: typing.Dict[str, typing.Dict[str, typing.Dict]] = {},
        **kwargs
    ) -> None:
        """

        На вход функции инициализации объекта передаются `dict` представления 
        подключений из Airflow: `airflow.models.connection.Connection.__dict__`
        вида {"login": "логин", "password": "пароль", "host": "0.0.0.0", ...}
        для подключения к хранилищу и сервисам, а также необходимые для работы
        менеджера коллекции в виде `Table` объекта.

        Функция определяет необходимые параметры логирования и инициализирует 
        доступные в секретах объекты модуля `updaters` – апдейтеры.
        Обращение и подключение к хранилищу выполняется через `self.connect()` 
        контекстный менеджер и сбрасывается по завершении его работы:
            with self.connect(...) as con:
                ... ### соединение подключено и может быть использовано
            ... ### соединение сброшено
                
        Аргументы:
            con (dict): параметры подключения к хранилищу
            cfg_table (Table): конфигурационная таблица для обработки сегментов
            log_table (Table): таблица для сохранения логов работы менеджера
            default_schema (str): дефолтная схема
            **kwargs: дополнительные параметры подключения,

        Возвращает:
            None
            
        """
        self.id = str(uuid4())
        self.sql_eng = create_engine(
            '{driver}://{login}:{password}@{host}:{port}/{schema}'.format(**con)
        )
        self.logger = Logger(self.id, Table(log_table), connect=self.connect)

        ### Декорирование используемых функций методом логгера
        global checks, refreshers, updaters
        checks, refreshers = [
            SimpleNamespace(
                **{k:self.logger.decorate(v) for k,v in vars(_).items() if inspect.isfunction(v)}
            ) for _ in (checks, refreshers)
        ]
        self.select_segments = self.logger.decorate(self.select_segments)
        self.refresh_segments = self.logger.decorate(self.refresh_segments)
        
        self.cfg_table = Table(cfg_table)
        checks.check_table(self.logger.table)
        checks.check_table(self.cfg_table, get_data=True)
        if not self.cfg_table.data.empty:
            self.cfg_table.data.table_name = self.cfg_table.data.table_name.map(Table, 'ignore')
    
        ### Инициализация менеджеров
        # self.managers = {k: Manager(id=k, **v) for k,v in consumers}

    @contextmanager
    def connect(self) -> typing.Generator[engine.Connection, typing.Any, None]:
        """
        Подключение к хранилищу данных
        ==============================

        """
        con: engine.Connection = self.sql_eng.connect()
        try:
            yield con
        finally:
            con.close()

    def select_segments(
        self, 
        **kwargs
    ) -> typing.Tuple[pd.DataFrame, typing.Union[bool, None]]:
        """
        Получение актуальных сегментов
        ==============================

        Декорируется `logger.decorate(...)` методом в рамках работы сегментера.

        Метод возвращает перечень готовых к пересчету сегментов. Такие сегменты
        должны:
        *   быть автообновляемыми: `refresh_auto` == true,
        *   иметь CRON-расписание: `refresh_cron` is not null,
        *   иметь параметры обновления: `refresh_params` != '{}'

        """
        return (self.cfg_table.data[
             self.cfg_table.data.refresh_auto & 
            ~self.cfg_table.data.refresh_cron.isna() &
            (self.cfg_table.data.refresh_params != {})
        ][
            ['segment_id', 'segment_name', 'table_name', 'refresh_cron', 'refresh_params']
        ].copy(), None)

    def refresh_segments(
        self,
        **kwargs
    ) -> typing.Tuple[pd.DataFrame, typing.Union[bool, None]]:
        """
        Пересчет сегментов
        ==================

        Декорируется `logger.decorate(...)` методом в рамках работы сегментера.
        
        Для каждого подходящего из `self.select_segments` сегмента проверить: 
            * наличие таблиц (check_table),
            * согласованность с конфигурационной таблицей (check_consistency),
            * соответстие по `refresh_cron` (check_cron),
            * TODO: прогруженность таблиц источников или любое другое условие, 
                реализованное в `checks` модуле
        и обновить сегмент соответствующим рефрешером.

        Возвращает:
            typing.Tuple[pd.DataFrame, typing.Union[bool, None]]:
                кортеж, содержащий датафрейм c переченем сегментов

        """
        global refreshers
        refreshed = pd.DataFrame()
        
        for _ in self.select_segments()[0].itertuples(index=False):
            skip = False
            for check in checks.__dict__.values():
                if not check(**_._asdict()):
                    skip = True
            if skip:
                continue
            refresh, params = (*_.refresh_params.items(),)[0]
            vars(refreshers)['refresh_' + refresh](**_._asdict(), **params)
            refreshed = pd.concat([refreshed, pd.DataFrame([_])], ignore_index=True)
        
        return (refreshed, None)

    # def select_audiences(self):
    #     """
    #     Получение перечня аудиторий
    #     ===========================
        
    #     """
    #     global updaters

    #     for consumer, target in self.cfg_table.data[
    #         ~(self.cfg_table.data.update_consumer.isna()) &
    #         ~(self.cfg_table.data.update_target.isna())
    #     ][
    #         ['update_consumer', 'update_target']
    #     ].itertuples():
    #         self.logger.decorate((self.updaters[target].select()))

    def update_audiences(self):
        """
        Обновление аудиторий
        ====================
        
        Для каждого актуального и успешно обновленного сегмента:

        """
        # for _ in self.managers:
        pass

        # consumer
            # segment
                # target