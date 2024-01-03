drop table "temp".temp_openweather ;
CREATE table if not exists "temp".temp_openweather (
	id serial8 NOT null primary key,
	city_id int8 NOT NULL,
	weather_main varchar NULL,
	weather_description varchar NULL,
	weather_temp varchar NULL,
	weather_temp_feels_like varchar NULL,
	weather_pressure varchar NULL,
	weather_humidity varchar NULL,
	weather_sea_level varchar NULL,
	weather_grnd_level varchar NULL,
	weather_wind_speed varchar NULL,
	weather_wind_gust varchar NULL,
	time_dim int8 NULL,
	time_sunrise timestamp null,
	time_sunset timestamp NULL,
	created_at timestamp null default now() ,
	updated_at timestamp NULL default now()
);