-- CREATE CITY_STATION TABLE

CREATE TABLE CITY_STATION (
    id SERIAL PRIMARY KEY,
    city_code VARCHAR(100) NOT NULL,
    station_code VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    UNIQUE (city_code)
);

-- INSERT DATA IN TABLE CITY_STATION

-- Comunidad Valenciana
INSERT INTO CITY_STATION (city_code, station_code, city_name, station_name)
VALUES
('46250', '8416', 'Valencia', 'VALÈNCIA'),
('03014', '8025', 'Alicante/Alacant', 'ALACANT/ALICANTE'),
('12040', '8500A', 'Castellón de la Plana/Castelló de la Plana', 'CASTELLÓ - ALMASSORA'),
('03099', '7244X', 'Orihuela', 'ORIHUELA'),

-- Región de Murcia
('30030', '7178I', 'Murcia', 'MURCIA'),
('30016', '7012C', 'Cartagena', 'CARTAGENA'),

-- Andalucía
('04013', '6325O', 'Almería', 'ALMERÍA AEROPUERTO'),
('29001', '6172X', 'Málaga', 'MÁLAGA'),
('14001', '5402', 'Córdoba', 'CÓRDOBA AEROPUERTO'),
('41001', '5783', 'Sevilla', 'SEVILLA AEROPUERTO'),
('18001', '5530E', 'Granada', 'GRANADA AEROPUERTO'),


-- Castilla-La Mancha
('02003', '8178D', 'Albacete', 'ALBACETE'),
('16001', '8096', 'Cuenca', 'CUENCA'),

-- Aragón
('44216', '8368U', 'Teruel', 'TERUEL'),
('22001', '9898', 'Huesca', 'HUESCA, AEROPUERTO'),
('50001', '9434', 'Zaragoza', 'ZARAGOZA, AEROPUERTO');


CREATE TABLE WEATHER_DATA (
    id SERIAL PRIMARY KEY,
    city_code VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    temperature_measured_avg DECIMAL(5, 2),
    temperature_measured_max DECIMAL(5, 2),
    temperature_measured_min DECIMAL(5, 2),
    temperature_predicted_avg DECIMAL(5, 2),
    temperature_predicted_max DECIMAL(5, 2),
    temperature_predicted_min DECIMAL(5, 2),
    FOREIGN KEY (city_code) REFERENCES CITY_STATION(city_code),
    UNIQUE (city_code, date)
);