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

INSERT INTO CITY_STATION (city_code, station_code, city_name, station_name)
VALUES

-- Valencia
('46250', '8416X', 'Valencia', 'VALENCIA, UPV'),

('46220', '8446Y', 'Sagunto', 'SAGUNT/SAGUNTO'),
('46145', '8293X', 'Xàtiva', 'XÀTIVA'),
('46071', '8270X', 'Bicorp', 'BICORP'),
('46083', '8300X', 'Carcaixent', 'CARCAIXENT'),
('46001', '8381X', 'Ademuz', 'ADEMUZ'),
('46248', '8337X', 'Turís', 'TURÍS'),
('46142', '8193E', 'Jalance', 'JALANCE'),
('46106', '8395X', 'Chelva', 'CHELVA'),
('46184', '8283X', 'Ontinyent', 'ONTINYENT'),
('46197', '8325X', 'Polinyà de Xúquer', 'POLINYÀ DE XÚQUER'),
('46046', '8072Y', 'Barx', 'BARX'),
('46168', '8058Y', 'Miramar', 'MIRAMAR'),
('46147', '8409X', 'Llíria', 'LLÍRIA'),
('46263', '8203O', 'Zarra', 'ZARRA'),
('46124', '8005X', 'Fontanars dels Alforins', 'FONTANARS DELS ALFORINS'),
('46249', '8309X', 'Utiel', 'UTIEL'),

-- Alicante
('03014', '8025', 'Alicante/Alacant', 'ALACANT/ALICANTE'),
('03031', '8036Y', 'Benidorm', 'BENIDORM'),
('03009', '8059C', 'Alcoy', 'ALCOI/ALCOY'),
('03099', '7244X', 'Orihuela', 'ORIHUELA'),
('03065', '8019', 'Elche', 'ALICANTE-ELCHE AEROPUERTO'),

('03140', '8008Y', 'Villena', 'VILLENA'),
('03102', '8057C', 'Pego', 'PEGO'),
('03093', '8013X', 'Novelda', 'NOVELDA'),
('03113', '7261X', 'Rojales', 'ROJALES'),
('03105', '7247X', 'Pinoso ', 'EL PINÓS/PINOSO'),
('03082', '8050X', 'Jávea', 'JÁVEA/ XÀBIA'),

-- Murcia
('30030', '7178I', 'Murcia', 'MURCIA'),
('30016', '7012C', 'Cartagena', 'CARTAGENA'),

('30024', '7209', 'Lorca', 'LORCA'),
('30026', '7007Y', 'Mazarrón', 'MAZARRÓN/LAS TORRES'),
('30003', '7002Y', 'Águilas', 'ÁGUILAS'),

('30001', '7250C', 'Abanilla', 'ABANILLA'),
('30039', '7218Y', 'Totana', 'TOTANA'),
('30013', '7121A', 'Calasparra', 'CALASPARRA'),
('30015', '7195X', 'Caravaca de la Cruz', 'CARAVACA DE LA CRUZ, LOS ROYOS'),
('30029', '7172X', 'Mula', 'MULA'),
('30037', '7026X', 'Torre-Pacheco', 'TORRE-PACHECO'),
('30027', '7237E', 'Molina de Segura', 'MOLINA DE SEGURA'),
('30043', '7275C', 'Yecla', 'YECLA'),
('30021', '7023X', 'Fuente Álamo de Murcia', 'FUENTE ÁLAMO DE MURCIA'),
('30009', '7158X', 'Archena', 'ARCHENA'),
('30005', '7228', 'Alcantarilla', 'ALCANTARILLA, BASE AÉREA'),
('30028', '7080X', 'Moratalla', 'MORATALLA'),
('30033', '7211B', 'Puerto Lumbreras', 'PUERTO LUMBRERAS'),
('30022', '7138B', 'Jumilla', 'JUMILLA'),
('30012', '7127X', 'Bullas', 'BULLAS'),
('30019', '7145D', 'Cieza', 'CIEZA'),
('30015', '7119B', 'Caravaca de la Cruz', 'CARAVACA DE LA CRUZ'),

-- Almería
('04013', '6325O', 'Almería', 'ALMERÍA AEROPUERTO'),

('04057', '6307X', 'Láujar de Andarax', 'LÁUJAR DE ANDARAX'),
('04003', '6277B', 'Adra', 'ADRA'),
('04001', '6302A', 'Abla', 'ABLA'),
('04006', '6364X', 'Albox', 'ALBOX'),
('04098', '5060X', 'Vélez-Blanco', 'VÉLEZ BLANCO - TOPARES'),
('04902', '6291B', 'El Ejido', 'EL EJIDO'),
('04049', '6340X', 'Garrucha', 'GARRUCHA, PUERTO'),
('04053', '6367B', 'Huércal-Overa', 'HUÉRCAL-OVERA');

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