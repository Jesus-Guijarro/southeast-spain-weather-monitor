-- CREATE CITY_STATION TABLE

CREATE TABLE CITIES (
    city_id SERIAL PRIMARY KEY,
    postal_code VARCHAR(100) NOT NULL,
    station_code VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    province VARCHAR(100) NOT NULL,
    UNIQUE (postal_code)
);

-- INSERT DATA IN TABLE CITY_STATION

INSERT INTO CITIES (postal_code, station_code, city_name, station_name, province)
VALUES

-- Valencia
('46250', '8416X', 'Valencia', 'VALENCIA, UPV', 'Valencia'),
('46220', '8446Y', 'Sagunto', 'SAGUNT/SAGUNTO', 'Valencia'),
('46145', '8293X', 'Xàtiva', 'XÀTIVA', 'Valencia'),
('46083', '8300X', 'Carcaixent', 'CARCAIXENT', 'Valencia'),
('46184', '8283X', 'Ontinyent', 'ONTINYENT', 'Valencia'),
('46147', '8409X', 'Llíria', 'LLÍRIA', 'Valencia'),
('46249', '8309X', 'Utiel', 'UTIEL', 'Valencia'),

-- Alicante
('03014', '8025', 'Alicante/Alacant', 'ALACANT/ALICANTE', 'Alicante'),
('03031', '8036Y', 'Benidorm', 'BENIDORM', 'Alicante'),
('03009', '8059C', 'Alcoy', 'ALCOI/ALCOY', 'Alicante'),
('03099', '7244X', 'Orihuela', 'ORIHUELA', 'Alicante'),
('03065', '8019', 'Elche', 'ALICANTE-ELCHE AEROPUERTO', 'Alicante'),
('03140', '8008Y', 'Villena', 'VILLENA', 'Alicante'),
('03102', '8057C', 'Pego', 'PEGO', 'Alicante'),
('03093', '8013X', 'Novelda', 'NOVELDA', 'Alicante'),
('03113', '7261X', 'Rojales', 'ROJALES', 'Alicante'),
('03082', '8050X', 'Jávea', 'JÁVEA/ XÀBIA', 'Alicante'),

-- Murcia
('30030', '7178I', 'Murcia', 'MURCIA', 'Murcia'),
('30016', '7012C', 'Cartagena', 'CARTAGENA', 'Murcia'),
('30024', '7209', 'Lorca', 'LORCA', 'Murcia'),
('30026', '7007Y', 'Mazarrón', 'MAZARRÓN/LAS TORRES', 'Murcia'),
('30003', '7002Y', 'Águilas', 'ÁGUILAS', 'Murcia'),
('30039', '7218Y', 'Totana', 'TOTANA', 'Murcia'),
('30013', '7121A', 'Calasparra', 'CALASPARRA', 'Murcia'),
('30015', '7195X', 'Caravaca de la Cruz', 'CARAVACA DE LA CRUZ, LOS ROYOS', 'Murcia'),
('30029', '7172X', 'Mula', 'MULA', 'Murcia'),
('30037', '7026X', 'Torre-Pacheco', 'TORRE-PACHECO', 'Murcia'),
('30027', '7237E', 'Molina de Segura', 'MOLINA DE SEGURA', 'Murcia'),
('30043', '7275C', 'Yecla', 'YECLA', 'Murcia'),
('30021', '7023X', 'Fuente Álamo de Murcia', 'FUENTE ÁLAMO DE MURCIA', 'Murcia'),
('30009', '7158X', 'Archena', 'ARCHENA', 'Murcia'),
('30005', '7228', 'Alcantarilla', 'ALCANTARILLA, BASE AÉREA', 'Murcia'),
('30033', '7211B', 'Puerto Lumbreras', 'PUERTO LUMBRERAS', 'Murcia'),
('30022', '7138B', 'Jumilla', 'JUMILLA', 'Murcia'),
('30012', '7127X', 'Bullas', 'BULLAS', 'Murcia'),
('30019', '7145D', 'Cieza', 'CIEZA', 'Murcia'),

-- Almeria

('04013', '6325O', 'Almería', 'ALMERÍA AEROPUERTO', 'Almeria'),
('04902', '6291B', 'El Ejido', 'EL EJIDO', 'Almeria'),
('04049', '6340X', 'Garrucha', 'GARRUCHA, PUERTO', 'Almeria'),
('04053', '6367B', 'Huércal-Overa', 'HUÉRCAL-OVERA', 'Almeria');


CREATE TABLE WEATHER_DATA (
    city_id INTEGER NOT NULL,
    date DATE NOT NULL,
    
    -- Prediction data
    temperature_predicted_avg REAL,
    temperature_predicted_max REAL,
    temperature_predicted_min REAL,
    humidity_predicted_avg REAL,
    humidity_predicted_max REAL,
    humidity_predicted_min REAL,
    precipitations JSONB,
    prob_precipitation JSONB,
    prob_storm JSONB,

    -- Meteo data
    temperature_measured_avg REAL,
    temperature_measured_max REAL,
    temperature_measured_min REAL,
    humidity_measured_avg REAL,
    humidity_measured_max REAL,
    humidity_measured_min REAL,
    precipitation REAL,

    FOREIGN KEY (city_id) REFERENCES CITIES(city_id) ON DELETE CASCADE,
    PRIMARY KEY (city_id, date)
);
