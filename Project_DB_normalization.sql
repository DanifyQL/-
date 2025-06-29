CREATE SCHEMA IF NOT EXISTS raw_data;

-- Этап 1. Загрузка данных и настройка связей.
CREATE TABLE IF NOT EXISTS raw_data.sales
(
	-- первичный ключ для дальнейшей нормализации БД
    id serial PRIMARY KEY, 
	-- поле с названием машины
    auto VARCHAR(50) NOT NULL, 
	-- есть дробный расход не превышающий десятичный разряд
    gasoline_consumption numeric(3,1) DEFAULT NULL,
	-- поле для записи цены автомобиля
    price numeric(18, 2) NOT NULL, 
	-- поле с датой продажи авто
    date date NOT NULL DEFAULT CURRENT_DATE,
	-- поле для записи имени клиента
    person_name VARCHAR(50) NOT NULL,
	-- поле для записи номера телефона
    phone text NOT NULL, 
	-- поле для фиксации была ли продана машина со скидкой. Скидка в целочисленном виде не превышает 100.
    discount SMALLINT NOT NULL DEFAULT 0,
	-- поле для записи страны бренда
    brand_origin VARCHAR(50), 
	-- цена продажи не может быть отрицательной или равна нулю
	CONSTRAINT price_positive CHECK (price > 0) 
);

-- загрузка данных в БД
COPY raw_data.sales
FROM 'C:\Temp\cars.csv' WITH CSV HEADER NULL 'null'; 

-- в данном пет-проекте машины у которых не указана страна-производитель будут обработаны специальной меткой - unknown
UPDATE 
	raw_data.sales AS s
SET
	brand_origin = 'unknown'
WHERE
	brand_origin IS NULL; 

-- тестовый запрос для дальнейшей нормализации БД. Вытаскиваем все необходимые компоненты. Будем использовать для конструкции INSERT INTO ... SELECT ...
SELECT
	*,
	-- отделяем марку машины
	TRIM(SPLIT_PART(auto, ' ', 1)) AS mark,
	-- отделяем модель машины
	SPLIT_PART(SUBSTR(auto, POSITION(' ' in auto) + 1), ',', 1) AS model,
	-- отделяем цвет модели машины
	TRIM(SUBSTR(auto, POSITION( ',' IN auto) + 1)) AS auto_color,
	-- разделяем имя и фамилию клиента на 2 разных поля
	SPLIT_PART(person_name, ' ', 1) AS first_name,
	SPLIT_PART(person_name, ' ', 2) AS last_name
FROM
	raw_data.sales 
LIMIT 1; 

/*
-- Проверяем длину строк атрибутов по отдельности для выбора типа данных и его длины.
SELECT
	MAX(LENGTH(TRIM(SPLIT_PART(auto, ' ', 1)))) AS max_mark_length, 
	MAX(LENGTH(SPLIT_PART(SUBSTR(auto, POSITION(' ' in auto) + 1), ',', 1))) AS max_model_length,
	MAX(LENGTH(TRIM(SUBSTR(auto, POSITION( ',' IN auto) + 1)))) AS max_color_length,
	MAX(LENGTH(SPLIT_PART(person_name, ' ', 1))) AS max_first_name_length,
	MAX(LENGTH(SPLIT_PART(person_name, ' ', 2))) AS max_last_name_length   
FROM
	raw_data.sales; 
*/

-- создаем схему для нормализации БД
CREATE SCHEMA IF NOT EXISTS car_shop; 

-- создаем таблицу с клиентами
CREATE TABLE IF NOT EXISTS car_shop.clients 
(
	id SERIAL PRIMARY KEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	phone TEXT NOT NULL
);

-- создаем справочник со странами
CREATE TABLE IF NOT EXISTS car_shop.countries
(
	id SERIAL PRIMARY KEY,
	brand_origin VARCHAR(30)
); 

-- создаем таблицу с брендами машин и связываем со справочником стран
CREATE TABLE IF NOT EXISTS car_shop.marks
(
	id SERIAL PRIMARY KEY,
	mark VARCHAR(50) NOT NULL,
	country_id int,
	FOREIGN KEY (country_id) REFERENCES car_shop.countries(id)
); 

 -- создаем таблицу с моделями машин и связываем со справочником брендов
CREATE TABLE IF NOT EXISTS car_shop.models
(
	model_id SERIAL PRIMARY KEY,
	model VARCHAR(50) NOT NULL,
	gasoline_consumption NUMERIC(3, 1) DEFAULT 0,
	mark_id int,
	FOREIGN KEY (mark_id) REFERENCES car_shop.marks(id)
);

-- создаем справочник с цветами машин
CREATE TABLE IF NOT EXISTS car_shop.colors 
(
	color_id SERIAL PRIMARY KEY,
	color VARCHAR(50)
); 

-- создаем основную таблицу с продажами и настраиваем связи
CREATE TABLE IF NOT EXISTS car_shop.sales
(
	id SERIAL PRIMARY KEY,
	client_id int,
	date DATE DEFAULT CURRENT_DATE,
	price NUMERIC(17, 2),
	discount SMALLINT NOT NULL DEFAULT 0,
	mark_id int,
	model_id int,
	color_id int,
	country_id int,
	CONSTRAINT price_positive CHECK (price > 0),
	FOREIGN KEY (mark_id) REFERENCES car_shop.marks(id),
	FOREIGN KEY (client_id) REFERENCES car_shop.clients(id),
	FOREIGN KEY (country_id) REFERENCES car_shop.countries(id),
	FOREIGN KEY (color_id) REFERENCES car_shop.colors(color_id),
	FOREIGN KEY (model_id) REFERENCES car_shop.models(model_id)
);


-- запросы для заполнения таблиц. 

INSERT INTO	car_shop.clients(first_name, last_name, phone)
-- вытаскиваем личные данные клиентов: имя, фамилия, телефон
SELECT DISTINCT 
	SPLIT_PART(person_name, ' ', 1) AS first_name,
	SPLIT_PART(person_name, ' ', 2) AS last_name,
	phone
FROM
	raw_data.sales
ORDER BY
	first_name, last_name;

 -- заполняем справочник стран уникальными значениями
INSERT INTO	car_shop.countries(brand_origin)
SELECT DISTINCT
	brand_origin
FROM
	raw_data.sales
ORDER BY
	brand_origin;

-- наполняем таблицу с брендами
INSERT INTO	car_shop.marks(mark, country_id) 
SELECT DISTINCT 
	TRIM(SPLIT_PART(auto, ' ', 1)) AS mark,
	c.id
FROM
	raw_data.sales s
LEFT JOIN
	car_shop.countries c ON c.brand_origin= s.brand_origin
ORDER BY
	mark;

 -- наполняем таблицу с моделями машин
INSERT INTO car_shop.models(model, gasoline_consumption, mark_id)
SELECT DISTINCT
	SPLIT_PART(SUBSTR(auto, POSITION(' ' in auto) + 1), ',', 1) AS model,
	gasoline_consumption,
	m.id
FROM
	raw_data.sales s
LEFT JOIN
	car_shop.marks m ON TRIM(SPLIT_PART(auto, ' ', 1)) = m.mark
ORDER BY
	model;

-- заполняем справочник с цветами машин
INSERT INTO car_shop.colors(color) 
SELECT DISTINCT 
	TRIM(SUBSTR(auto, POSITION( ',' IN auto) + 1)) AS color
FROM
	raw_data.sales s
ORDER BY
	color;

 -- заполняем основную таблицу с продажами
INSERT INTO car_shop.sales(client_id, date, price, discount, mark_id, model_id, color_id, country_id)
SELECT
	csc2.id AS client_id,
	date,
	price,
	discount,
	csm.id AS mark_id,
	csm1.model_id AS model_id,
	csc.color_id AS color_id,
	csc1.id AS country_id
FROM
	raw_data.sales rds
JOIN
 	car_shop.clients csc2 ON csc2.phone = rds.phone
JOIN
 	car_shop.marks csm ON csm.mark = TRIM(SPLIT_PART(auto, ' ', 1))
JOIN
 	car_shop.models csm1 ON csm1.model = SPLIT_PART(SUBSTR(auto, POSITION(' ' in auto) + 1), ',', 1)
JOIN
 	car_shop.countries csc1 ON csc1.brand_origin = rds.brand_origin
JOIN
	car_shop.colors csc ON csc.color = TRIM(SUBSTR(auto, POSITION( ',' IN auto) + 1));

-- Этап 2. Тестовые запросы
-- Запрос. Выводим ср. цену машины по бренду по годам
SELECT DISTINCT
	csm.mark AS brand_name,
	EXTRACT(YEAR FROM css.date) AS year,
	ROUND(AVG(css.price) OVER (PARTITION BY csm.mark, EXTRACT(YEAR FROM css.date)), 2) AS price_avg
FROM
	car_shop.sales css
JOIN
	car_shop.marks csm ON css.mark_id = csm.id
GROUP BY
	brand_name, year, css.price
ORDER BY
	brand_name, year;

-- Запрос. Выводим помесячно среднюю цену авто за 2022 год
SELECT DISTINCT
	EXTRACT(MONTH FROM css.date) AS month,
	EXTRACT(YEAR FROM css.date) AS year,
	ROUND(AVG(css.price) OVER (PARTITION BY EXTRACT(MONTH FROM css.date)), 2) AS price_avg
FROM
	car_shop.sales css
WHERE
	EXTRACT(YEAR FROM css.date) = 2022
GROUP BY
	month, year, css.price
ORDER BY
	month;

-- Запрос. Выводим по каждому из пользователей в системы машины через запятую, которые он приобрел
SELECT 
	csc.first_name || ' ' || csc.last_name AS person,
	TRIM(STRING_AGG(csm.mark || ' ' || csm1.model, ', ')) AS cars
FROM
	car_shop.sales css
JOIN
	car_shop.clients csc ON css.client_id = csc.id
JOIN
	car_shop.marks csm ON css.mark_id = csm.id
JOIN
	car_shop.models csm1 ON css.model_id = csm1.model_id
GROUP BY
	person
ORDER BY
	person;

-- Запрос. Считаем по каждому из брендов максимальную и минимальную цену без учета скидки. 
-- Исключаем данные, где не указана страна-производитель. Т.к. запрос тестовый и разовый CTE не используем.
SELECT 
	q.brand_origin AS brand_origin,
	MAX(q.price_wo_discount) AS price_max,
	MIN(q.price_wo_discount) AS price_min
FROM 
(
	SELECT 
		csc.brand_origin AS brand_origin,
		CASE
			WHEN css.discount = 0 THEN ROUND(price, 2)
			ELSE ROUND((css.price / (1 - discount::numeric / 100)), 2)
		END AS price_wo_discount
	FROM
		car_shop.sales css
	JOIN
		car_shop.countries csc ON css.country_id = csc.id
) AS q
WHERE
	brand_origin != 'unknown'
GROUP BY
	brand_origin
ORDER BY
	price_max DESC;

-- Запрос. Считаем кол-во клиентов из США
SELECT DISTINCT
	COUNT(css.id) AS persons_from_usa_count
FROM
	car_shop.sales css
JOIN
	car_shop.clients csc ON css.client_id = csc.id
WHERE
	csc.phone LIKE '+1%';

-- быстрое удаление тестовой БД
/*
DROP TABLE IF EXISTS raw_data.sales;
DROP SCHEMA IF EXISTS car_shop CASCADE;
DROP SCHEMA IF EXISTS raw_data CASCADE;
*/ 