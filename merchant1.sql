/*
Задание
Эту задачу попросил решить один из ключевых клиентов финансового стартапа <...>.
Есть 3 таблицы:

clients:
client_id      client_name      is_actual
  1	              Ivanov	          1
  2	              Petrov	          1
  ...              ...            ...

client_passport_change_log:
client_id      date_change      passport_value
1	            2019-02-01	      pass 1
1	            2019-03-05	      pass 2
1	            2019-12-15	      pass 3
2	            2019-05-01	      pass 11
2	            2019-08-01	 
2	            2020-12-01	      pass 21
...              ...            ...

client_address_change_log:
client_id	      date_change	      address_value
1              	2019-01-01	      addr 1
1	              2019-03-05	      addr 2
1	              2019-12-01	      addr 3
2	              2019-05-01	      addr 11
2	              2019-08-01	 
...                ...              ...

В результате необходимо сформировать так называемую таблицу SCD - slowly changed dimension. 
Другими словами, это таблица, которая показывает, в какие интервалы времени какое значение было у каждого атрибута.

Ответ должен содержать поля:
client_id - идентификатор клиента
client_name - имя клиента
valid_from - дата, когда эта запись вступила в действие
valid_to - дата, когда эта запись стала неактуальна
address_value - значение адреса
passport_value - значение паспорта
*/

with t1 as (
	select 
		a.client_id ,
		a.date_change ,
		coalesce(a.address_value, '#empty#') as address_value,	--если запись в смене адреса есть, но нулевая (дата есть, а значения присвоенного нет), то выводится эмпти
		null as passport_value --строка заглушка для использования юнион
	from client_address_change_log a
	union
	select 
		p.client_id ,
		p.date_change ,
		null as address_value,	--строка заглушка для использования юнион
		coalesce(p.passport_value, '#empty#') as passport_value
	from client_passport_change_log p
),
	t2 as (
	select
		client_id,
		date_change,
		max(address_value) as address_value, --отбираем значения для повторяющихся дат, чтобы исключить null при имеющихся числовых
		max(passport_value) as passport_value
	from t1
	group by client_id, date_change
),
	t3 as (
	select 
		client_id,
		date_change,
		lead(date_change, 1, '6000-01-01') over(partition by client_id order by date_change) - 1 as valid_to,  --ищет в следующей строке дату следующего изменения данных, если не находит, то дефолтной конечной датой идет 6000-01-01
		address_value,
		passport_value,
		sum(case when address_value is not null then 1 else 0 end) over(partition by client_id order by date_change) as partition_address,  --суммирует ненулевые значения изменений данных, чтобы получить актуальную позицию изменения данных на текущую дату
		sum(case when passport_value is not null then 1 else 0 end) over(partition by client_id order by date_change) as partition_passport
	from t2
)
select
	t3.client_id,
	c.client_name,
	date_change as valid_from,
	valid_to,
	--address_value,
	--passport_value,
	--partition_address,
	--partition_passport,
	case 
		when first_value(address_value) over(partition by t3.client_id, partition_address order by date_change) <> '#empty#' 
			then first_value(address_value) over(partition by t3.client_id, partition_address order by date_change)
	end as address_value, --если ближайшее значение по искомому партишну не равно эмпти, то выводит его, иначе null
	case 
		when first_value(passport_value) over(partition by t3.client_id, partition_passport order by date_change) <> '#empty#' 
			then first_value(passport_value) over(partition by t3.client_id, partition_passport order by date_change)
	end as passport_value
from t3
	join clients c on t3.client_id = c.client_id
