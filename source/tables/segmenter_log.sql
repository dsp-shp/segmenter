/*
	Таблица "segmenter_log" – коллекция, хранящая результаты выполнения тех или 
	иных действий, совершаемых сегментером.
*/

drop table if exists public.segmenter_log;
create table public.segmenter_log (
	processed 			timestamp	not null	default now(),
	"action" 			varchar 	null,
	"target" 			varchar 	null,
	id 					varchar 	null,
	segment_id 			varchar 	null,
	segment_name 		varchar 	null,
	coverage 			int 		null,
	first_upload_dttm 	timestamp 	null,
	last_upload_dttm 	timestamp 	null,
	num_received 		int 		null,
	num_invalid 		int 		null,
	contact 			varchar 	null,
	error 				varchar 	null,
	existed 			boolean 	null
);

comment on table public.segmenter_log               is 'Логовая таблица';
comment on column public.segmenter_log.processed    is 'Время логирования записи';
comment on column public.segmenter_log.action       is 'Некоторое действие сегментера';
comment on column public.segmenter_log.target       is 'Целевая система для передачи сегмента';
comment on column public.segmenter_log.id           is 'Идентификатор сегмента';
comment on column public.segmenter_log.segment_id   is 'Идентификатор сегмента на стороне клиента';
comment on column public.segmenter_log.segment_name is 'Наименование сегмента на стороне клиента';
comment on column public.segmenter_log.coverage     is 'Размер аудитории';