{{ config(materialized='table') }}
with daily_weather as (
    select 
    date(time) as daily_weather,weather,CLOUDS,HUMIDITY,PRESSURE,TEMP 
    from 
    {{ source('demo', 'weather') }}

), 
daily_weatherAGG AS (
    SELECT 
    daily_weather,
    weather,
   ROUND(AVG(CLOUDS),2) AVG_CLOUDS,
   ROUND(AVG(HUMIDITY),2) AVG_HUMIDITY,
   ROUND(AVG(TEMP),2) AVG_TEMP,
   ROUND(AVG(PRESSURE),2) AVG_PRESSURE
    FROM daily_weather
    GROUP BY  weather,daily_weather
    QUALIFY ROW_NUMBER() OVER (PARTITION BY daily_weather ORDER BY COUNT(weather) DESC)=1
    
)
select * from daily_weatherAGG