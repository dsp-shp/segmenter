/*
	Таблица "segmenter_segments" – конфигурационная коллекция для задания 
	сегментов и параметров их обновления.
*/

drop table if exists public.segmenter_segments;
create table public.segmenter_segments (
	id				uuid 		not null 	default gen_random_uuid(),
	"name"			varchar 	not null,
	description 	varchar 	null,
	table_name 		varchar 	not null,
	context 		varchar 	null,
	actual_begin 	timestamp 	not null 	default now(),
	actual_end 		timestamp 	not null 	default 'infinity'::timestamp,
	refresh_auto 	bool 		not null 	default false,
	refresh_params 	json 		null 		default '{}'::json,
	refresh_cron 	varchar 	null,
	constraint segmenter_segments_pkey primary key (id)
);

comment on table public.segmenter_segments                  is 'Таблица сегментов'; 
comment on column public.segmenter_segments.id              is 'Суррогатный первичный ключ, некоторый случайно сгенерированный идентификатор';
comment on column public.segmenter_segments.name            is 'Наименование сегмента';
comment on column public.segmenter_segments.description     is 'Описание сегмента';
comment on column public.segmenter_segments.table_name      is 'Таблица, содержащая сегмент в формате "схема.таблица"';
comment on column public.segmenter_segments.context         is 'Связанная задача';
comment on column public.segmenter_segments.actual_begin    is 'Дата заведения сегмента';
comment on column public.segmenter_segments.actual_end      is 'Актуальность сегмента';
comment on column public.segmenter_segments.refresh_auto    is 'Является ли сегмент автообновляемым: "false" – для обновляемых вручную, "true" – для автообновляемых';
comment on column public.segmenter_segments.refresh_params  is 'Параметры создания или пересчета сегмента: "query" – запрос для пересчета сегмента, "procedure" – наименование функции пересчета сегмента';
comment on column public.segmenter_segments.refresh_cron    is 'CRON-расписание пересчета сегмента';