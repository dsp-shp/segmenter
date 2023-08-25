from . import checks, refreshers, updaters
from .utils import Logger, Table
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
        __init__: инициализация объекта, сервисных апдейтеров и логирования,
            подключение к хранилищу данных, проверка доступности коллекций
        connect (contextmanager): подключение к хранилищу данных
        select_segments: получение актуальных сегментов
        refresh_segments: batch-пересчет сегментов
        select_audiences: получение перечня аудиторий (из кабинета)
        update_audiences: обновление аудиторий
        ### run: основная рабочая функция, выполняющая весь набор обработок

    """
    def __init__(
        self, 
        con: dict,
        cfg_table: str = 'segmenter',
        log_table: str = 'segmenter_log',
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
        self.logger = Logger(self.id, log_table, connect=self.connect)

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
        self.cfg_table.data.table_name = self.cfg_table.data.table_name.map(Table, 'ignore')

        return

        ### Инициализация необходимых загрузчиков
        self.updaters = {
            _: vars(updaters)[_](**kwargs.get(_.lower() + '_con', {})) \
                for _ in self.cfg.update_target.tolist() if _
        }
        # self.logger.info('Инициализация загрузчиков: {}'.format([*self.updaters.keys()]))

    @contextmanager
    def connect(self) -> engine.base.Connection:
        """
        Подключение к хранилищу данных
        ==============================

        """
        con = self.sql_eng.connect()
        try:
            yield con
        finally:
            con.close()

    def select_segments(
        self, 
        **kwargs
    ) -> typing.Tuple[pd.core.frame.DataFrame, typing.Union[bool, None]]:
        """
        Получение актуальных сегментов
        ==============================

        Декорируется `logger.decorate(...)` методом в рамках работы сегментера.

        Метод возвращает перечень готовых к пересчету сегментов. Такие сегменты
        должны:
            быть автообновляемыми: `refresh_auto` == true,
            иметь CRON-расписание: `refresh_cron` is not null,
            иметь параметры обновления: `refresh_params` != '{}'

        """
        return (self.cfg_table.data[
             self.cfg_table.data.refresh_auto & 
            ~self.cfg_table.data.refresh_cron.isna() &
            (self.cfg_table.data.refresh_params != {})
        ][
            ['id', 'name', 'table_name', 'refresh_cron', 'refresh_params']
        ].copy(),)

    def refresh_segments(
        self,
        **kwargs
    ) -> typing.Tuple[pd.core.frame.DataFrame, typing.Union[bool, None]]:
        """
        Пересчет сегментов
        ==================

        Декорируется `logger.decorate(...)` методом в рамках работы сегментера.
        
        Для каждого подходящего из `self.select_segments` сегмента проверить: 
            наличие таблиц,
            соответстие по `refresh_cron`,
            прогруженность таблиц источников,
            ... или любое другое условие, реализованное в `checks` модуле
        и обновить сегмент соответствующим рефрешером.

        """
        global refreshers
        refreshed = pd.DataFrame()
        
        for _ in self.select_segments().itertuples(index=False):
            for check in checks.__dict__.values():
                if not check(**_._asdict()):
                    continue
            refresh, params = (*_.refresh_params.items(),)[0]
            vars(refreshers)['refresh_' + refresh](_.table_name, **params)
            refreshed = pd.concat([refreshed, pd.DataFrame([_])], ignore_index=True)
        
        return (refreshed,)

    def select_audiences(self):
        """
        Получение перечня аудиторий
        ===========================
        
        """
        global updaters

        data, audiences = pd.DataFrame(), []

        for x in updaters:
            try:

                audiences.append(vars(updaters)[x].select())
            except:
                pass

        data = pd.concat(audiences).reset_index(drop=True)

    def update_audiences(self):
        """
        Обновление аудиторий
        ====================
        
        Для каждого актуального и успешно обновленного сегмента:

        """
        pass