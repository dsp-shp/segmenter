/*
    Представление "public.segmenter" – ...
*/

drop view if exists public.segmenter;
create view public.segmenter as
with actual_subscriptions as (
	select subs.*
	from segmenter_subscriptions subs
	left join segmenter_consumers cons on subs.consumer_id = cons.id
	where coalesce(cons.actual_end, 'infinity') = 'infinity' and subs.actual_end = 'infinity'
)
select base.id
	 , base."name"
	 , base.description
	 , base.table_name
	 , base.context
	 , base.refresh_auto
	 , base.refresh_params
	 , base.refresh_cron
	 , subs.consumer_id as update_consumer
	 , subs.target as update_target
	 , subs.name as update_name
	 , subs.params as update_params
     , subs.cron as update_cron
from segmenter_segments segm
left join actual_subscriptions subs on segm.id = subs.segment_id and subs.actual_end = 'infinity'
left join segmenter_log on false
where segm.actual_end = 'infinity'
order by id, update_consumer, update_target
;

comment on view public.segmenter                    is 'Сборная витрина';
comment on column public.segmenter.id               is 'Идентификатор сегмента';
comment on column public.segmenter.name             is 'Наименование сегмента';
comment on column public.segmenter.description      is 'Описание сегмента';
comment on column public.segmenter.table_name       is 'Таблица, содержащая сегмент в формате "схема.таблица"';
comment on column public.segmenter.context          is 'Связанная задача';
comment on column public.segmenter.refresh_auto     is 'Является ли сегмент автообновляемым: "false" – для обновляемых вручную, "true" – для автообновляемых';
comment on column public.segmenter.refresh_params   is 'Параметры создания или пересчета сегмента: "query" – запрос для пересчета сегмента, "procedure" – наименование функции пересчета сегмента';
comment on column public.segmenter.refresh_cron     is 'CRON-расписание пересчета сегмента';
comment on column public.segmenter.update_consumer  is 'Идентификатор потребителя';
comment on column public.segmenter.update_target    is 'Целевая система';
comment on column public.segmenter.update_name      is 'Наименование аудитории';
comment on column public.segmenter.update_params    is 'Параметры обновления аудитории';
comment on column public.segmenter.update_cron      is 'CRON-расписание обновления аудитории';