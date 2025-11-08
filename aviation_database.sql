-- =============================================
-- ПРОЕКТ: СИСТЕМА УПРАВЛЕНИЯ АВИАПЕРЕВОЗКАМИ
-- БАЗА ДАННЫХ: PostgreSQL
-- СХЕМА: bookings
-- =============================================

-- === ЭТАП 1: СОЗДАНИЕ ТАБЛИЦЫ CITIES ===

-- Создание таблицы cities
CREATE TABLE bookings.cities (
    city_id SERIAL NOT NULL,
    city_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    timezone TEXT,
    CONSTRAINT cities_pkey PRIMARY KEY (city_id)
);

-- Заполнение таблицы данными из airports
INSERT INTO bookings.cities (city_name, latitude, longitude, timezone)
SELECT 
    city,
    AVG(latitude) as latitude,
    AVG(longitude) as longitude,
    MAX(timezone) as timezone
FROM bookings.airports 
GROUP BY city;

-- Модификация таблицы cities
-- 1. Добавление двух новых полей
ALTER TABLE bookings.cities 
ADD COLUMN airport_count INTEGER,
ADD COLUMN departure_count INTEGER;

-- 2. Изменение типа данных для city_name
ALTER TABLE bookings.cities 
ALTER COLUMN city_name TYPE VARCHAR(255);

-- 3. Добавление ограничения на airport_count
ALTER TABLE bookings.cities 
ADD CONSTRAINT airport_count_nonnegative 
CHECK (airport_count >= 0);

-- Обновление данных в новых полях
UPDATE bookings.cities c
SET airport_count = (
    SELECT COUNT(*) 
    FROM bookings.airports a 
    WHERE a.city = c.city_name
);

UPDATE bookings.cities c
SET departure_count = (
    SELECT COUNT(*) 
    FROM bookings.flights f
    JOIN bookings.airports a ON f.departure_airport = a.airport_code
    WHERE a.city = c.city_name
);


-- === ЭТАП 2: СОЗДАНИЕ ТАБЛИЦЫ ROUTES ===

-- Создание таблицы routes
CREATE TABLE bookings.routes (
    flight_no CHAR(6) PRIMARY KEY,
    departure_airport CHAR(3) NOT NULL,
    arrival_airport CHAR(3) NOT NULL,
    aircraft_code CHAR(3) NOT NULL,
    duration INTERVAL NOT NULL
);

-- Заполнение таблицы данными из flights
INSERT INTO bookings.routes (flight_no, departure_airport, arrival_airport, aircraft_code, duration)
SELECT DISTINCT 
    f.flight_no,
    f.departure_airport,
    f.arrival_airport,
    f.aircraft_code,
    (f.scheduled_arrival - f.scheduled_departure) AS duration
FROM bookings.flights f
WHERE f.flight_no IS NOT NULL
  AND f.departure_airport IS NOT NULL
  AND f.arrival_airport IS NOT NULL
  AND f.aircraft_code IS NOT NULL
  AND f.scheduled_departure IS NOT NULL
  AND f.scheduled_arrival IS NOT NULL;


-- === ЭТАП 3: СОЗДАНИЕ VIEW FLIGHT_AIRPORT_INFO ===

-- Создание представления flight_airport_info
CREATE VIEW bookings.flight_airport_info AS
SELECT 
    f.flight_no,
    f.flight_id,
    f.scheduled_departure,
    f.scheduled_arrival,
    f.actual_departure,
    f.actual_arrival,
    f.status,
    f.aircraft_code,
    
    -- Информация об аэропорте вылета
    dep.airport_code AS departure_airport_code,
    dep.airport_name AS departure_airport_name,
    dep.city AS departure_city,
    dep.longitude AS departure_longitude,
    dep.latitude AS departure_latitude,
    dep.timezone AS departure_timezone,
    
    -- Информация об аэропорте прилета
    arr.airport_code AS arrival_airport_code,
    arr.airport_name AS arrival_airport_name,
    arr.city AS arrival_city,
    arr.longitude AS arrival_longitude,
    arr.latitude AS arrival_latitude,
    arr.timezone AS arrival_timezone
    
FROM bookings.flights f
JOIN bookings.airports dep ON f.departure_airport = dep.airport_code
JOIN bookings.airports arr ON f.arrival_airport = arr.airport_code;


-- === ЭТАП 4: ОБРАБОТКА ДАННЫХ ИЗ JSON ===

-- Создание таблицы bookings
CREATE TABLE bookings.bookings (
    book_ref CHAR(6) NOT NULL PRIMARY KEY,
    book_date TIMESTAMPTZ NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL
);

-- Заполнение таблицы bookings данными из bookings_json
INSERT INTO bookings.bookings (book_ref, book_date, total_amount)
SELECT DISTINCT 
    book_ref,
    book_date,
    total_amount
FROM bookings_json;

-- Создание таблицы tickets_text с текстовыми полями
CREATE TABLE bookings.tickets_text (
    ticket_no TEXT,
    book_ref TEXT,
    passenger_id TEXT,
    passenger_name TEXT,
    contact_data TEXT
);

-- Заполнение таблицы tickets_text данными из bookings_json
INSERT INTO bookings.tickets_text (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
SELECT 
    (ticket->>'ticket_no') AS ticket_no,
    bj.book_ref,
    ticket->>'passenger_id' AS passenger_id,
    ticket->>'passenger_name' AS passenger_name,
    (ticket->'contact_data')::TEXT AS contact_data
FROM bookings_json bj
CROSS JOIN LATERAL jsonb_array_elements(bj.json_data::JSONB) AS ticket;

-- Создание VIEW tickets с явными типами
CREATE VIEW bookings.tickets AS
SELECT 
    ticket_no::BPCHAR(13) AS ticket_no,
    book_ref::BPCHAR(6) AS book_ref,
    passenger_id::VARCHAR(20) AS passenger_id,
    passenger_name::TEXT AS passenger_name,
    contact_data::JSONB AS contact_data
FROM bookings.tickets_text;

-- Создание таблицы ticket_flights_raw
CREATE TABLE bookings.ticket_flights_raw (
    ticket_no TEXT,
    flight_id TEXT,
    fare_conditions TEXT,
    amount TEXT
);

-- Заполнение таблицы ticket_flights_raw данными из bookings_json
INSERT INTO bookings.ticket_flights_raw (ticket_no, flight_id, fare_conditions, amount)
SELECT 
    (ticket->>'ticket_no') AS ticket_no,
    (flight->>'flight_id') AS flight_id,
    (flight->>'fare_conditions') AS fare_conditions,
    (flight->>'amount') AS amount
FROM bookings_json bj
CROSS JOIN LATERAL jsonb_array_elements(bj.json_data::JSONB) AS ticket
CROSS JOIN LATERAL jsonb_array_elements(ticket->'flights') AS flight;

-- Создание VIEW ticket_flights с правильными типами данных
CREATE VIEW bookings.ticket_flights AS
SELECT 
    ticket_no::BPCHAR(13) AS ticket_no,
    flight_id::INTEGER AS flight_id,
    fare_conditions::VARCHAR(10) AS fare_conditions,
    amount::NUMERIC(10,2) AS amount
FROM bookings.ticket_flights_raw;