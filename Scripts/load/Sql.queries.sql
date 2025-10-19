select * from air_quality;
select * from population;
select * from weather;

ALTER TABLE weather
ADD CONSTRAINT weather_datetime_city_unique UNIQUE (weather_datetime, city);

	
-- Check latest dates in your table
SELECT MAX(aqi_datetime) FROM air_quality;

-- Check date format of a few rows
SELECT aqi_datetime, city, aqi FROM air_quality ORDER BY aqi_datetime DESC LIMIT 5;


ALTER TABLE air_quality
ALTER COLUMN aqi_datetime TYPE TIMESTAMP WITH TIME ZONE;

ALTER TABLE air_quality
ADD CONSTRAINT unique_city_datetime UNIQUE (city, aqi_datetime);


---1️.Latest AQI & Pollution Severity per City
CREATE VIEW latest_city_aqi AS
SELECT 
    city,
    country,
    aqi,
    pollution_severity,
    health_risk,
    aqi_datetime
FROM air_quality a
WHERE aqi_datetime = (
    SELECT MAX(aqi_datetime) FROM air_quality a2 
	WHERE a2.city = a.city
);
select * from latest_city_aqi;


---2.Daily Average AQI per City

CREATE VIEW daily_city_aqi AS
SELECT
    city,
    DATE(aqi_datetime) AS date,
    AVG(aqi) AS avg_aqi,
    AVG(pm25) AS avg_pm25,
    AVG(pm10) AS avg_pm10
FROM air_quality
GROUP BY city, DATE(aqi_datetime)
ORDER BY city, date;
select * from daily_city_aqi;

---3️.Health Risk Summary per City

CREATE VIEW health_risk_summary AS
SELECT
    city,
    health_risk,
    COUNT(*) AS hours_in_category
FROM air_quality
GROUP BY city, health_risk
ORDER BY health_risk asc;
select * from health_risk_summary;

---4.Top Polluted Cities (Average AQI)

CREATE VIEW top_polluted_cities AS
SELECT
    city,
    AVG(aqi) AS avg_aqi
FROM air_quality
WHERE aqi_datetime >= NOW() - INTERVAL '30 days'
GROUP BY city
ORDER BY avg_aqi DESC;
select * from top_polluted_cities;

---5️.AQI vs Weather Correlation

CREATE VIEW aqi_weather_correlation AS
SELECT
    a.city,
    a.aqi,
    w.temperature,
    w.humidity,
    w.wind_speed,
    w.clouds,
    w.weather_description,
    a.aqi_datetime
FROM air_quality a
JOIN weather w
ON a.city = w.city AND DATE_TRUNC('hour', a.aqi_datetime)
= DATE_TRUNC('hour', w.weather_datetime);
select * from aqi_weather_correlation;
drop view aqi_weather_correlation


--6.Rolling Averages / Trend Smoothing
CREATE VIEW rolling_aqi_trends AS
SELECT
    city,
    aqi_datetime,
    aqi_rolling_24h,
    pm25_rolling_24h
FROM air_quality
ORDER BY city, aqi_datetime;
select * from rolling_aqi_trends;

--7.Unhealthy PM2.5 Hours per City
CREATE VIEW unhealthy_pm25_hours AS
SELECT
    city,
    COUNT(*) AS hours_unhealthy_pm25
FROM air_quality
WHERE unhealthy_pm25 = TRUE
GROUP BY city
ORDER BY hours_unhealthy_pm25 DESC;
select * from unhealthy_pm25_hours;



