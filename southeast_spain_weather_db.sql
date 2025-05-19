-- CREATE southeast_spain_weather DATABASE
CREATE DATABASE southeast_spain_weather;

-- CREATE provinces TABLE
CREATE TABLE provinces (
  province_id SERIAL PRIMARY KEY,
  name VARCHAR(100) UNIQUE NOT NULL
);

-- INSERT DATA IN provinces
INSERT INTO provinces (name)
VALUES
  ('Valencia'),
  ('Alicante'),
  ('Murcia'),
  ('Almería');

-- CREATE cities TABLE
CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,
    postal_code VARCHAR(100) NOT NULL,
    station_code VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    province_id INTEGER NOT NULL
      REFERENCES provinces(province_id)
      ON DELETE RESTRICT,
    UNIQUE (postal_code)
);

-- INSERT DATA IN cities
INSERT INTO cities (postal_code, station_code, city_name, station_name, province_id)
VALUES
-- Valencia
('46250', '8416Y', 'Valencia', 'VALÈNCIA, VIVEROS', 1),
('46220', '8446Y', 'Sagunto', 'SAGUNT/SAGUNTO', 1),
('46145', '8293X', 'Xàtiva', 'XÀTIVA', 1),
('46083', '8300X', 'Carcaixent', 'CARCAIXENT', 1),
('46184', '8283X', 'Ontinyent', 'ONTINYENT', 1),
('46147', '8409X', 'Llíria', 'LLÍRIA', 1),
('46249', '8309X', 'Utiel', 'UTIEL', 1),
('46168', '8058Y', 'Miramar', 'MIRAMAR', 1),

-- Alicante
('03014', '8025', 'Alicante/Alacant', 'ALACANT/ALICANTE', 2),
('03031', '8036Y', 'Benidorm', 'BENIDORM', 2),
('03009', '8059C', 'Alcoy', 'ALCOI/ALCOY', 2),
('03099', '7244X', 'Orihuela', 'ORIHUELA', 2),
('03065', '8018X', 'Elche', 'ELX/ELCHE', 2),
('03140', '8008Y', 'Villena', 'VILLENA', 2),
('03102', '8057C', 'Pego', 'PEGO', 2),
('03093', '8013X', 'Novelda', 'NOVELDA', 2),
('03113', '7261X', 'Rojales', 'ROJALES', 2),
('03082', '8050X', 'Jávea', 'JÁVEA/ XÀBIA', 2),

-- Murcia
('30030', '7178I', 'Murcia', 'MURCIA', 3),
('30016', '7012D', 'Cartagena', 'CARTAGENA', 3),
('30024', '7209', 'Lorca', 'LORCA', 3),
('30026', '7007Y', 'Mazarrón', 'MAZARRÓN/LAS TORRES', 3),
('30003', '7002Y', 'Águilas', 'ÁGUILAS', 3),
('30039', '7218Y', 'Totana', 'TOTANA', 3),
('30013', '7121A', 'Calasparra', 'CALASPARRA', 3),
('30015', '7195X', 'Caravaca de la Cruz', 'CARAVACA DE LA CRUZ, LOS ROYOS', 3),
('30029', '7172X', 'Mula', 'MULA', 3),
('30037', '7026X', 'Torre-Pacheco', 'TORRE-PACHECO', 3),
('30027', '7237E', 'Molina de Segura', 'MOLINA DE SEGURA', 3),
('30043', '7275C', 'Yecla', 'YECLA', 3),
('30021', '7023X', 'Fuente Álamo de Murcia', 'FUENTE ÁLAMO DE MURCIA', 3),
('30009', '7158X', 'Archena', 'ARCHENA', 3),
('30005', '7228', 'Alcantarilla', 'ALCANTARILLA, BASE AÉREA', 3),
('30033', '7211B', 'Puerto Lumbreras', 'PUERTO LUMBRERAS', 3),
('30022', '7138B', 'Jumilla', 'JUMILLA', 3),
('30012', '7127X', 'Bullas', 'BULLAS', 3),
('30019', '7145D', 'Cieza', 'CIEZA', 3),

-- Almeria
('04013', '6325O', 'Almería', 'ALMERÍA AEROPUERTO', 4),
('04032', '6332Y', 'Carboneras', 'CARBONERAS', 4),
('04902', '6291B', 'El Ejido', 'EL EJIDO', 4),
('04049', '6340X', 'Garrucha', 'GARRUCHA, PUERTO', 4),
('04053', '6367B', 'Huércal-Overa', 'HUÉRCAL-OVERA', 4),
('04079', '6293X', 'Roquetas de Mar', 'ROQUETAS DE MAR', 4),
('04057', '6307X', 'Láujar de Andarax', 'LÁUJAR DE ANDARAX', 4),
('04006', '6364X', 'Albox', 'ALBOX', 4);



-- CREATE weather_data TABLE
CREATE TABLE weather_data (
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