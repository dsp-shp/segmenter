## Сегментер

Фреймворк, осуществляющий полный цикл приготовления сегмента, начиная с процесса создания или обновления его в источнике и заканчивая передачей его внешним пользователям и системам.

### **Зависимости**
[https://github.com/dsp-shp/segmenter/blob/27139f1fc140372707e4270e87289bd60585351d/requirements.txt#L1C1-L1C1](https://github.com/dsp-shp/segmenter/blob/a35f864deac2632ced44a7c70de3b25fcaf8be71/requirements.txt#L1-L13)

### **Установка**

Фреймворк устанавливается следующей командой:
```bash
pip install git+https://github.com/dsp-shp/segmenter.git
pip show segmenter 
```

Для работы сегментера также необходимо создание в базе данных 5 коллекций:
- логовой *"public.segmenter_log"*,
- конфигурационных:
	- *"public.segmenter_segments"* – для сегментов,
	- *"public.segmenter_subscriptions"* – для подписок на сегменты,
	- *"public.segmenter_consumers"* – для подписчиков,
- одной сборной *"public.segmenter"* витрины.

Примеры DDL запросов для их создания можно найти в /source/tables/ директории.
> *ПРИМЕЧАНИЕ: Сегментер рассчитан на взаимодействие с postgres или postgres-подобными СУБД. Использование любой другой реляционной системы управления возможно при условии корректировки DDL (см. segmenter/tables), и DML (находящихся в компонентах фреймворка) скриптов под необходимый SQL диалект.*

### **Рабочий цикл**
Жизненный цикл сегментера начинается с определения всех актуальных, готовых к рефрешу
1. Для каждого актуального сегмента – ```actual_end = 'infinity'```:
	- проверить 
	- проверить соответстие по ```refresh_cron```
	- проверить прогруженность таблиц источников
	- обновить сегмент соответствующим рефрешером
	- залогировать обновление или ошибку обновления в логовую таблицу
1. Для каждого апдейтера получить перечень аудиторий
1. Для каждого актуального и успешно обновленного сегмента:
	- ...

### **Добавление сегмента**
```sql
drop table if exists public.test_segment; 
create table public.test_segment (
    segment_id      uuid        not null,
    id              varchar     null,
    hash_email      varchar     null,
    hash_phone      varchar     null,
    actual_begin    timestamp   not null    default now(),
    actual_end      timestamp   not null    default 'infinity',
    processed       timestamp   not null    default now(),
    constraint test_segment_id_key unique (id)
);
create index on test_segment using btree(id);
```
