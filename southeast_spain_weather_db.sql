-- CREATE southeast_spain_weather DATABASE
CREATE DATABASE southeast_spain_weather;

-- CREATE provinces TABLE
CREATE TABLE provinces (
  province_id SERIAL PRIMARY KEY,
  name VARCHAR(70) UNIQUE NOT NULL
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
    postal_code VARCHAR(5) NOT NULL,
    station_code VARCHAR(6) NOT NULL,
    city_name VARCHAR(70) NOT NULL,
    station_name VARCHAR(70) NOT NULL,
    latitude NUMERIC(9,6) NOT NULL,
    longitude NUMERIC(9,6) NOT NULL,
    province_id INTEGER NOT NULL
      REFERENCES provinces(province_id)
      ON DELETE RESTRICT,
    UNIQUE (postal_code)
);

-- INSERT DATA IN cities
INSERT INTO cities (postal_code, station_code, city_name, station_name, latitude, longitude, province_id)
VALUES
-- Valencia
('46250', '8416Y', 'Valencia', 'VALÈNCIA, VIVEROS',39.480339,-0.368115,1),
('46220', '8446Y', 'Sagunto', 'SAGUNT/SAGUNTO',39.680084,-0.275960,1),
('46145', '8293X', 'Xàtiva', 'XÀTIVA',38.990276,-0.521111,1),
('46083','8300X','Carcaixent','CARCAIXENT',39.122761,-0.446980,1),
('46184','8283X','Ontinyent','ONTINYENT',38.821000,-0.610547,1),
('46147','8409X','Llíria','LLÍRIA',39.633333,-0.600000,1),
('46168','8058Y','Miramar','MIRAMAR',38.951694,-0.138787,1),

-- Alicante
('03014','8025','Alicante/Alacant','ALACANT/ALICANTE',38.345200,-0.481500,2),
('03031','8036Y','Benidorm','BENIDORM',38.534168,-0.131389,2),
('03009','8059C','Alcoy','ALCOI/ALCOY',38.702906,-0.471958,2),
('03099','7244X','Orihuela','ORIHUELA',38.084831,-0.943000,2),
('03065','8018X','Elche','ELX/ELCHE',38.269932,-0.712561,2),
('03140','8008Y','Villena','VILLENA',38.637300,-0.865680,2),
('03102','8057C','Pego','PEGO',38.843236,-0.117553,2),
('03093','8013X','Novelda','NOVELDA',38.384790,-0.767730,2),
('03113','7261X','Rojales','ROJALES',38.088611,-0.723611,2),
('03082','8050X','Jávea','JÁVEA/ XÀBIA',38.783330, 0.166670,2),

-- Murcia
('30030','7178I','Murcia','MURCIA',37.984047,-1.128575,3),
('30016','7012D','Cartagena','CARTAGENA',37.599998,-0.983333,3),
('30024','7209','Lorca','LORCA',37.671189,-1.701700,3),
('30026','7007Y','Mazarrón','MAZARRÓN/LAS TORRES',37.597636,-1.316686,3),
('30003','7002Y','Águilas','ÁGUILAS',37.406300,-1.582889,3),
('30039','7218Y','Totana','TOTANA',37.766667,-1.500000,3),
('30013','7121A','Calasparra','CALASPARRA',38.229970,-1.699860,3),
('30015','7195X','Caravaca de la Cruz','CARAVACA DE LA CRUZ, LOS ROYOS',38.107500,-1.860000,3),
('30029','7172X','Mula','MULA',38.050000,-1.500000,3),
('30037','7026X','Torre-Pacheco','TORRE-PACHECO',37.744239,-0.953626,3),
('30027','7237E','Molina de Segura','MOLINA DE SEGURA',38.050000,-1.216700,3),
('30043','7275C','Yecla','YECLA',38.616700,-1.116700,3),
('30021','7023X','Fuente Álamo de Murcia','FUENTE ÁLAMO DE MURCIA',37.723870,-1.170201,3),
('30009','7158X','Archena','ARCHENA',38.116310,-1.300430,3),
('30005','7228','Alcantarilla','ALCANTARILLA, BASE AÉREA',37.969400,-1.217100,3),
('30033','7211B','Puerto Lumbreras','PUERTO LUMBRERAS',37.566700,-1.816700,3),
('30022','7138B','Jumilla','JUMILLA',38.475950,-1.322032,3),
('30012','7127X','Bullas','BULLAS',38.043345,-1.673640,3),
('30019','7145D','Cieza','CIEZA',38.242091,-1.421727,3),

-- Almeria
('04013','6325O','Almería','ALMERÍA AEROPUERTO',36.839700,-2.368700,4),
('04032','6332Y','Carboneras','CARBONERAS',36.996586,-1.895012,4),
('04902','6291B','El Ejido','EL EJIDO',36.775761,-2.814908,4),
('04049','6340X','Garrucha','GARRUCHA, PUERTO',37.179250,-1.821901,4),
('04053','6367B','Huércal-Overa','HUÉRCAL-OVERA',37.388969,-1.945361,4),
('04079','6293X','Roquetas de Mar','ROQUETAS DE MAR',36.764678,-2.615057,4),
('04057','6307X','Láujar de Andarax','LÁUJAR DE ANDARAX',36.992335,-2.899644,4),
('04006','6364X','Albox','ALBOX',37.388507,-2.147991,4);


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