/*
	Таблица "segmenter_consumers" – конфигурационная коллекция для хранения информации о внешних потребителях сегмента.
*/

drop table if exists public.segmenter_consumers;
create table public.segmenter_consumers (
	id 				uuid 		not null	default gen_random_uuid(),
	"name" 			varchar 	null,
	description 	varchar 	null, 
		/* ... описание и любые другие атрибуты подписчика ... */
	actual_begin 	timestamp 	not null 	default now(),
	actual_end 		timestamp 	not null 	default 'infinity'::timestamp,
	constraint segmenter_pkey primary key (id)
);

comment on table public.segmenter_consumers                 is 'Таблица потребителей';
comment on column public.segmenter_consumers.id             is 'Суррогатный первичный ключ, некоторый случайно сгенерированный идентификатор';
comment on column public.segmenter_consumers.name           is 'Наименование потребителя';
comment on column public.segmenter_consumers.description    is 'Описание потребителя';
comment on column public.segmenter_consumers.actual_begin   is 'Дата заведения сегмента';
comment on column public.segmenter_consumers.actual_end     is 'Актуальность сегмента';