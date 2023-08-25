/*
	Таблица "segmenter_subscriptions" – конфигурационная коллекция, связывающая 
	сегментную и потребительскую коллекции, является фактом наличия подписки 
	некоторым внешним пользователем на некоторый сегмент.
*/

drop table if exists public.segmenter_subscriptions;
create table public.segmenter_subscriptions (
	consumer_id 	uuid 		not null,
	segment_id 		uuid 		not null,
	target 			varchar 	not null,
	"name" 			varchar 	not null 	default '{{name}}',
	params 			json 		not null 	default '{}'::json,
	cron 			varchar 	null,
	status 			varchar 	not null 	default 'not uploaded',
	actual_begin 	timestamp 	not null 	default now(),
	actual_end 		timestamp 	not null 	default 'infinity'::timestamp
);

comment on table public.segmenter_subscriptions                 is 'Таблица подписок';
comment on column public.segmenter_subscriptions.consumer_id    is 'Идентификатор потребителя';
comment on column public.segmenter_subscriptions.segment_id     is 'Идентификатор сегмента';
comment on column public.segmenter_subscriptions.target         is 'Целевая система';
comment on column public.segmenter_subscriptions.name           is 'Наименование аудитории';
comment on column public.segmenter_subscriptions.params         is 'Параметры обновления аудитории';
comment on column public.segmenter_subscriptions.cron           is 'CRON-расписание обновления аудитории';
comment on column public.segmenter_subscriptions.status         is 'Статус обновления';
comment on column public.segmenter_subscriptions.actual_begin   is 'Дата заведения сегмента';
comment on column public.segmenter_subscriptions.actual_end     is 'Актуальность сегмента';