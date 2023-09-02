from .logger import Logger
import pandas as pd
import typing

class Target:
    """
    Таргет
    ======
    
    Абстрактный класс, содержащий методы взаимодействия с таргетом.

    Методы:
        ...

    """
    def __init__(
        self,
        logger: Logger,
        **kwargs
    ) -> None:
        self.logger = logger
        self.parse = logger.decorate(self.parse)
        self.select = logger.decorate(self.select)
        self.update = logger.decorate(self.update)
        self.create = logger.decorate(self.create)
        self.remove = logger.decorate(self.remove)

    def create(self):
        pass
    
    def parse(self):
        pass

    def remove(self):
        pass
    
    def select(self):
        pass
    
    def update(self):
        pass



    
