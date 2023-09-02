from .. import updaters
from .table import Table
from .target import Target
from sqlalchemy import engine
import typing
import pandas as pd

class Manager:
    """
    Менеджер
    ========

    Объект, содержащий методы взаимодействия с потребителем сегментов.
    Конфигурируется через public.segmenter_consumers коллекцию.

    Методы:
        ###
        
    Свойства:
        id: идентификатор потребителя в хранилище данных
        targets: перечень актуальных для потребителя таргетов

    """

    def __init__(
        self,
        id: str,
        targets: dict,
        table: Table = None,
        connect: engine.Connection = None
    ) -> None:
        self._id = id
        self.table = table
        
        if not connect:
            self.warning('Не передан метод подключения к хранилищу')
            return
        self.connect = connect

        if self.connect:
            with self.connect() as con:
                self.table = pd.read_sql("""
                    select update_consumer as id
                         , update_target as target
                         , update_name as name
                         , update_params as params
                         , update_cron as cron
                         , udpate_remove as remove
                    from segmenter
                    where update_consumer = '{id}';
                """.format(id=id), con)

        # df['update_name'] = df.apply(lambda x: x['update_name'].format(**x.to_dict()), axis=1)

        self._targets = {
            k: vars(updaters)[k](**v) for k, v in targets
        }

    @property
    def id(self) -> str: return self._id

    @property
    def targets(self) -> typing.Dict[str, Target]: return self._targets

    # def create(self):
    #     pass
    
    # def parse(self):
    #     pass

    # def remove(self):
    #     pass
    
    # def select(self):
    #     pass
    
    # def update(self):
    #     pass



