"""
Проверки
========

Модуль содержит набор следующих проверок сегментера:
    consistency – проверка соответствия конфигурационному файлу
    cron – проверка готовности по CRON-расписанию
    relevance - проверка актуальности сегмента
    table – проверка доступности коллекции

"""

from .cron import check_cron
from .table import check_table
from .consistency import check_consistency
# from .relevance import check_relevance