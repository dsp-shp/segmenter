"""
Сегментер
=========

Модуль позволяет автоматизировать процесс управления сегментами в маркетинговых
сервисах, реализуя актуализацию переченя групп, создание, обновление (выгрузку) 
и удаление пользовательских сегментов.

"""
from .source.segmenter import Segmenter
from .source.utils import Logger, Table