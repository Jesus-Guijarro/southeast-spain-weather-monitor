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
('46250', '8416X', 'Valencia', 'VALENCIA, UPV'),
('03014', '8025', 'Alicante/Alacant', 'ALACANT/ALICANTE'),
('12040', '8500A', 'Castellón de la Plana/Castelló de la Plana', 'CASTELLÓ - ALMASSORA'),
('03099', '7244X', 'Orihuela', 'ORIHUELA'),

-- Región de Murcia
('30030', '7178I', 'Murcia', 'MURCIA'),
('30016', '7012C', 'Cartagena', 'CARTAGENA'),

-- Comunidad de Madrid
('28079', '3195', 'Madrid', 'MADRID, RETIRO'),

-- Cataluña
('43148', '0042Y', 'Tarragona', 'TARRAGONA'),
('08019', '0201D', 'Barcelona', 'BARCELONA'),

-- Islas Canarias
('38038', 'C449C', 'Santa Cruz de Tenerife', 'STA. CRUZ DE TENERIFE'),
('35016', 'C658X', 'Las Palmas', 'LAS PALMAS DE GRAN CANARIA, TAFIRA'),

-- Andalucía
('04013', '6325O', 'Almería', 'ALMERÍA AEROPUERTO'),
('29067', '6172X', 'Málaga', 'MÁLAGA'),
('14021', '5402', 'Córdoba', 'CÓRDOBA AEROPUERTO'),
('41091', '5783', 'Sevilla', 'SEVILLA AEROPUERTO'),
('18087', '5530E', 'Granada', 'GRANADA AEROPUERTO'),

-- Castilla-La Mancha
('02003', '8178D', 'Albacete', 'ALBACETE'),
('16078', '8096', 'Cuenca', 'CUENCA'),

-- Aragón
('22125', '8368U', 'Teruel', 'TERUEL'),
('22001', '9898', 'Huesca', 'HUESCA, AEROPUERTO'),
('50297', '9434', 'Zaragoza', 'ZARAGOZA, AEROPUERTO'),

-- Islas Baleares
('07040', 'B278', 'Palma de Mallorca', 'PALMA DE MALLORCA, AEROPUERTO');


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