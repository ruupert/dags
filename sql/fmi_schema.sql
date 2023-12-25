CREATE TABLE IF NOT EXISTS fcast_loc (
                            fcast_loc_id INT GENERATED ALWAYS AS IDENTITY,
                            name TEXT,
                            fmisid INTEGER,
                            latitude REAL,
                            longitude REAL,
                            PRIMARY KEY(fcast_loc_id));
CREATE TABLE IF NOT EXISTS fcast (
                            id INT GENERATED ALWAYS AS IDENTITY,
                            fcast_loc_id INT,
                            date timestamp NOT NULL,
                            temp REAL NOT NULL,
                            hpa REAL NOT NULL,
                            humidity REAL NOT NULL,
                            geo_potential_h REAL NOT NULL,
                            u_component_wind REAL NOT NULL,
                            v_component_wind REAL NOT NULL,
                            rain_mm_hr REAL NOT NULL,
                            CONSTRAINT fk_fcast_loc
                                FOREIGN KEY(fcast_loc_id)
                                    REFERENCES fcast_loc(fcast_loc_id)); 
CREATE UNIQUE INDEX IF NOT EXISTS idx_fcast_loc_name ON fcast_loc (name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fcast_date ON fcast (fcast_loc_id, date);
-- obs tables
CREATE TABLE IF NOT EXISTS loc (
                            loc_id INT,
                            name TEXT,
                            latitude REAL,
                            longitude REAL,
                            PRIMARY KEY(loc_id));
CREATE TABLE IF NOT EXISTS obs (
                            id INT GENERATED ALWAYS AS IDENTITY,
                            loc_id INT,
                            date timestamp NOT NULL,
                            temp_c REAL,
                            wind_speed_ms REAL,
                            wind_gust_ms REAL,
                            wind_direction_deg REAL,
                            humidity REAL,
                            dew_point_temp_c REAL,
                            precipitation_mm REAL,
                            precipitation_mmh REAL,
                            snow_depth_cm REAL,
                            pressure_hpa REAL,
                            horiz_vis_m REAL,
                            cloud_amount REAL,
                            present_weather REAL,
                            CONSTRAINT fk_loc
                                FOREIGN KEY(loc_id)
                                    REFERENCES loc(loc_id));
CREATE UNIQUE INDEX IF NOT EXISTS idx_loc_name ON loc (name);
CREATE UNIQUE INDEX IF NOT EXISTS idx_date ON obs (loc_id, date);
