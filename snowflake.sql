--Database and Schema Creation-------------------------------

CREATE OR REPLACE DATABASE northwoods;

USE DATABASE northwoods;

CREATE OR REPLACE SCHEMA pov_reporting;

USE SCHEMA pov_reporting;



--Table Creation---------------------------------------------

CREATE OR REPLACE TABLE airlines (iata_code string, 
                                  airline string) 
COMMENT = 'Airline codes and names';

CREATE OR REPLACE TABLE airports (iata_code string, 
                                  airport string,
                                  city string,
                                  state string,
                                  country string,
                                  latitude number,
                                  longitude number)
COMMENT = 'Airport codes, names, and locations';
                      
CREATE OR REPLACE TABLE flights (year number,
                                 month number,
                                 day number,
                                 day_of_week number,
                                 airline string,
                                 flight_number string,
                                 tail_number string,
                                 origin_airport string,
                                 destination_airport string,
                                 scheduled_departure_time string,
                                 departure_time string,
                                 departure_delay number,
                                 taxi_out number,
                                 wheels_off string,
                                 elapsed_time number,
                                 scheduled_time number,
                                 air_time number,
                                 distance number,
                                 wheels_on number,
                                 taxi_in number,
                                 scheduled_arrival number,
                                 arrival_time string,
                                 arrival_delay string,
                                 diverted number,
                                 cancelled number,
                                 cancellation_reason string,
                                 air_system_delay number,
                                 security_delay number,
                                 airline_delay number,
                                 late_aircraft_delay number,
                                 weather_delay number) 
COMMENT = 'Flight Records';

CREATE OR REPLACE TABLE cancellation_reasons (code string, text string);
INSERT INTO cancellation_reasons VALUES ('A','Airline/Carrier');
INSERT INTO cancellation_reasons VALUES ('B','Weather');
INSERT INTO cancellation_reasons VALUES ('C','National Air System');
INSERT INTO cancellation_reasons VALUES ('D','Security');




--Table Population------------------------------------------
/*  
--Below is no longer needed as table population is handled via databricks


COPY INTO airlines
  FROM 'azure://frontier05a31ae603fe420b.blob.core.windows.net/landing/airlines'
  CREDENTIALS=(AZURE_SAS_TOKEN= 'xxxx')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO airports
  FROM 'azure://frontier05a31ae603fe420b.blob.core.windows.net/landing/airports'
  CREDENTIALS=(AZURE_SAS_TOKEN='xxx')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

COPY INTO flights
  FROM 'azure://frontier05a31ae603fe420b.blob.core.windows.net/landing/flights'
  CREDENTIALS=(AZURE_SAS_TOKEN='xxx')
  FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);


SELECT 'airlines', COUNT(1) FROM airlines
UNION
SELECT 'airports', COUNT(1) FROM airports
UNION
SELECT 'flights', COUNT(1) FROM flights;
*/

--View Creation--------------------------------------------


--Total number of flights by airline and airport on a monthly basis
CREATE OR REPLACE VIEW num_flights_by_airline_airport AS
WITH a AS (SELECT origin_airport AS airport, airline, month, year from flights
            UNION ALL
           SELECT destination_airport AS airport, airline, month, year from flights)
SELECT airlines.airline,airports.airport, a.month, a.year, COUNT(1) AS flights
  FROM a
 INNER JOIN airlines ON airlines.iata_code = a.airline
 INNER JOIN airports ON airports.iata_code = a.airport
 GROUP BY airlines.airline,airports.airport, a.month, a.year
 ORDER BY airlines.airline, airports.airport, a.year,a.month;


--Total number of flights by airline monthly basis
CREATE OR REPLACE VIEW num_flights_by_airline AS
SELECT airlines.airline, flights.month, flights.year, COUNT(1) AS flights
FROM flights
INNER JOIN airlines ON airlines.iata_code = flights.airline
GROUP BY airlines.airline, flights.month, flights.year
ORDER BY airlines.airline, flights.year, flights.month;


--On time percentage of each airline for the year 2015
CREATE OR REPLACE VIEW on_time_percentage_by_airline AS
SELECT airlines.airline,
       round((1- (COUNT(CASE WHEN flights.departure_delay > 0 OR flights.arrival_delay > 0 THEN 1 ELSE null END) / COUNT(1))) * 100, 2) AS on_time_percentage  --Computing the percentage of online flights
FROM flights
INNER JOIN airlines ON airlines.iata_code = flights.airline
WHERE flights.year = 2015
GROUP BY airlines.airline
ORDER BY 2 DESC;


--Airlines with the largest number of delays
CREATE OR REPLACE VIEW largest_num_of_delays_by_airline AS
SELECT airlines.airline, 
       count(1) AS number_of_delayed_flights
  FROM flights
 INNER JOIN airlines ON airlines.iata_code = flights.airline
 WHERE flights.departure_delay > 0 OR flights.arrival_delay > 0
 GROUP BY airlines.airline
ORDER BY 2 DESC;


--Cancellation reasons by airport
CREATE OR REPLACE VIEW cancellation_reasons_by_airport AS
WITH a AS (SELECT DISTINCT origin_airport as airport, cancellation_reason 
             FROM flights
            WHERE cancellation_reason IS NOT null)

SELECT airports.airport, 
       listagg(cancellation_reasons.text,',') WITHIN GROUP (ORDER BY cancellation_reason) as cancellation_reasons
  FROM a
 INNER JOIN cancellation_reasons ON cancellation_reasons.code = cancellation_reason
 INNER JOIN airports ON airports.iata_code = a.airport
 GROUP BY airports.airport
 ORDER BY 1;

--Cancellations by reasons and airports
CREATE OR REPLACE VIEW cancellations_by_reasons_and_airports AS
SELECT airports.airport, 
       cancellation_reasons.text AS cancellation_reason,
       COUNT(1) cancellation_count
  FROM flights
 INNER JOIN cancellation_reasons ON cancellation_reasons.code = flights.cancellation_reason
 INNER JOIN airports ON airports.iata_code = flights.origin_airport
 WHERE flights.cancellation_reason IS NOT null
 GROUP BY airports.airport, cancellation_reasons.text


--Delay reasons by airport
CREATE OR REPLACE VIEW delay_reasons_by_airport AS
WITH a AS (SELECT origin_airport as airport,
                  air_system_delay,
                  security_delay,
                  airline_delay,
                  late_aircraft_delay,
                  weather_delay
            FROM flights
           WHERE air_system_delay IS NOT NULL),--If any of the delay columns are NULL, then they all are and the record doesn't contain any delay information

b as (SELECT airport, 'Air System' AS delay_reason FROM a WHERE air_system_delay > 0 
       UNION
      SELECT airport, 'Security' AS delay_reason FROM a WHERE security_delay > 0 
       UNION
      SELECT airport, 'Airline' AS delay_reason FROM a WHERE airline_delay > 0 
       UNION
      SELECT airport, 'Late Aircraft' AS delay_reason FROM a WHERE late_aircraft_delay > 0 
       UNION
      SELECT airport, 'Weather' AS delay_reason FROM a WHERE weather_delay > 0 )
SELECT airports.airport, 
       listagg(delay_reason,',') WITHIN GROUP (ORDER BY delay_reason) as delay_reasonS
FROM b
INNER JOIN airports ON airports.iata_code = b.airport
GROUP BY airports.airport;


--Delay reason counts by airport
CREATE OR REPLACE VIEW delay_reason_counts_by_airport AS
WITH a AS (SELECT origin_airport as airport,
                  air_system_delay,
                  security_delay,
                  airline_delay,
                  late_aircraft_delay,
                  weather_delay
            FROM flights
           WHERE air_system_delay IS NOT NULL),--If any of the delay columns are NULL, then they all are and the record doesn't contain any delay information

b as (SELECT airport, 'Air System' AS delay_reason FROM a WHERE air_system_delay > 0 
       UNION ALL
      SELECT airport, 'Security' AS delay_reason FROM a WHERE security_delay > 0 
       UNION ALL
      SELECT airport, 'Airline' AS delay_reason FROM a WHERE airline_delay > 0 
       UNION ALL
      SELECT airport, 'Late Aircraft' AS delay_reason FROM a WHERE late_aircraft_delay > 0 
       UNION ALL
      SELECT airport, 'Weather' AS delay_reason FROM a WHERE weather_delay > 0 )
SELECT airports.airport, 
       delay_reason,
       COUNT(1) AS delays
  FROM b
 INNER JOIN airports ON airports.iata_code = b.airport
 WHERE delay_reason IS NOT NULL
 GROUP BY airports.airport,delay_reason


--Airline with the most unique routes
CREATE OR REPLACE VIEW airline_most_unique_routes AS
WITH a AS (SELECT DISTINCT origin_airport, destination_airport, airline FROM flights), --First, find all the distinct route combinations
     b AS (SELECT airlines.airline, COUNT(1) --Then count the comtinations by airline and return the top result
             FROM a
            INNER JOIN airlines ON airlines.iata_code = a.airline
            GROUP BY airlines.airline
            ORDER BY 2 DESC
            LIMIT 1)
SELECT airline  --Finally, return just the airline name               
  FROM b;
           
--Unique Routes by Airline
CREATE OR REPLACE VIEW airline_unique_routes AS
WITH a AS (SELECT DISTINCT origin_airport, destination_airport, airline 
             FROM flights) --First, find all the distinct route combinations
SELECT airlines.airline, 
       COUNT(1) AS routes --Then count the comtinations by airline 
  FROM a
 INNER JOIN airlines ON airlines.iata_code = a.airline
 GROUP BY airlines.airline
 ORDER BY 2 DESC
