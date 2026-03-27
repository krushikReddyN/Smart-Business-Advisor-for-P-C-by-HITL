-- =============================================================================
-- HITL Smart P&C Advisor — Complete Data Setup Script
-- Database: HITL_SBA_DB | Schema: PC_INSURANCE
-- Run this entire block once to create all tables, stages, and seed data.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS HITL_SBA_DB;
CREATE SCHEMA IF NOT EXISTS HITL_SBA_DB.PC_INSURANCE;

-- -----------------------------------------------------------------------------
-- 1. PROPERTY_MASTER (10,000 rows) — includes POLICY_NUMBER & PROVIDER_NAME
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER AS
WITH params AS (
    SELECT
        ARRAY_CONSTRUCT('Miami','Tampa','Jacksonville','Houston','Galveston','New Orleans','Charleston','Savannah','Norfolk','San Diego','Los Angeles','San Francisco','Sacramento','Portland','Seattle','Phoenix','Denver','Dallas','Oklahoma City','Kansas City','Chicago','Detroit','Minneapolis','Atlanta','Nashville','Charlotte','Boston','New York','Philadelphia','Baltimore') AS cities,
        ARRAY_CONSTRUCT('FL','FL','FL','TX','TX','LA','SC','GA','VA','CA','CA','CA','CA','OR','WA','AZ','CO','TX','OK','MO','IL','MI','MN','GA','TN','NC','MA','NY','PA','MD') AS states,
        ARRAY_CONSTRUCT('33101','33602','32099','77001','77550','70112','29401','31401','23501','92101','90001','94102','95814','97201','98101','85001','80201','75201','73101','64101','60601','48201','55401','30301','37201','28201','02101','10001','19101','21201') AS zips,
        ARRAY_CONSTRUCT(25.76,27.95,30.33,29.76,29.30,29.95,32.78,32.08,36.85,32.72,34.05,37.77,38.58,45.52,47.61,33.45,39.74,32.78,35.47,39.10,41.88,42.33,44.98,33.75,36.16,35.23,42.36,40.71,39.95,39.29) AS lats,
        ARRAY_CONSTRUCT(-80.19,-82.46,-81.66,-95.37,-94.79,-90.07,-79.93,-81.09,-76.29,-117.16,-118.24,-122.42,-121.49,-122.68,-122.33,-112.07,-104.99,-96.80,-97.52,-94.58,-87.63,-83.05,-93.27,-84.39,-86.78,-80.84,-71.06,-74.01,-75.17,-76.61) AS lngs,
        ARRAY_CONSTRUCT('Main St','Oak Ave','Elm Dr','Maple Ln','Pine Rd','Cedar Blvd','Walnut St','Cherry Way','Birch Ave','Willow Dr','Park Ave','Lake St','River Rd','Forest Dr','Hill St','Valley Rd','Sunset Blvd','Ocean Ave','Harbor Dr','Bay St') AS streets,
        ARRAY_CONSTRUCT('Frame','Masonry','Steel','Concrete','Mixed') AS constructions
),
base AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn,
        UNIFORM(0, 29, RANDOM()) AS city_idx,
        UNIFORM(100, 9999, RANDOM()) AS street_num,
        UNIFORM(0, 19, RANDOM()) AS street_idx,
        UNIFORM(1, 100, RANDOM()) AS prop_type_pct,
        UNIFORM(1940, 2024, RANDOM()) AS yr_built,
        UNIFORM(0, 40, RANDOM()) AS raw_roof_age,
        UNIFORM(0, 4, RANDOM()) AS const_idx
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT
    'PROP-' || LPAD(b.rn::STRING, 5, '0') AS property_id,
    'POL-' || LPAD(UNIFORM(100000, 999999, RANDOM())::STRING, 6, '0') || '-' || p.states[b.city_idx]::STRING AS policy_number,
    CASE UNIFORM(1, 10, RANDOM())
        WHEN 1 THEN 'State Farm' WHEN 2 THEN 'Allstate' WHEN 3 THEN 'USAA'
        WHEN 4 THEN 'Progressive' WHEN 5 THEN 'Liberty Mutual' WHEN 6 THEN 'Chubb'
        WHEN 7 THEN 'Travelers' WHEN 8 THEN 'Nationwide' WHEN 9 THEN 'Farmers'
        ELSE 'Hartford'
    END AS provider_name,
    b.street_num::STRING || ' ' || p.streets[b.street_idx]::STRING AS address,
    p.cities[b.city_idx]::STRING AS city,
    p.states[b.city_idx]::STRING AS state,
    p.zips[b.city_idx]::STRING AS zip,
    ROUND(p.lats[b.city_idx]::FLOAT + UNIFORM(-0.05::FLOAT, 0.05::FLOAT, RANDOM()), 4) AS latitude,
    ROUND(p.lngs[b.city_idx]::FLOAT + UNIFORM(-0.05::FLOAT, 0.05::FLOAT, RANDOM()), 4) AS longitude,
    CASE WHEN b.prop_type_pct <= 70 THEN 'Residential' ELSE 'Commercial' END AS property_type,
    b.yr_built AS year_built,
    LEAST(b.raw_roof_age, 2026 - b.yr_built) AS roof_age,
    p.constructions[b.const_idx]::STRING AS construction_type,
    CASE
        WHEN b.prop_type_pct <= 70 THEN UNIFORM(100, 2000, RANDOM()) * 1000
        ELSE UNIFORM(500, 15000, RANDOM()) * 1000
    END AS insured_value
FROM base b
CROSS JOIN params p;

-- -----------------------------------------------------------------------------
-- 2. AERIAL_ANALYSIS (10,000 rows)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.AERIAL_ANALYSIS AS
WITH region_map AS (
    SELECT column1 AS city, column2 AS region_type
    FROM VALUES
        ('Miami','coastal'),('Tampa','coastal'),('Jacksonville','coastal'),
        ('Houston','coastal'),('Galveston','coastal'),('New Orleans','coastal'),
        ('Charleston','coastal'),('Savannah','coastal'),('Norfolk','coastal'),
        ('San Diego','coastal'),('Los Angeles','wildfire'),('San Francisco','coastal'),
        ('Sacramento','wildfire'),('Portland','inland'),('Seattle','coastal'),
        ('Phoenix','arid'),('Denver','inland'),('Dallas','tornado'),
        ('Oklahoma City','tornado'),('Kansas City','tornado'),
        ('Chicago','inland'),('Detroit','inland'),('Minneapolis','inland'),
        ('Atlanta','inland'),('Nashville','inland'),('Charlotte','inland'),
        ('Boston','coastal'),('New York','coastal'),('Philadelphia','inland'),
        ('Baltimore','coastal')
),
base AS (
    SELECT
        p.property_id,
        p.property_type,
        p.year_built,
        p.roof_age,
        p.construction_type,
        COALESCE(rm.region_type, 'inland') AS region,
        UNIFORM(1, 100, RANDOM()) AS veg_pct
    FROM HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER p
    LEFT JOIN region_map rm ON p.city = rm.city
)
SELECT
    property_id,
    ROUND(LEAST(1.0, GREATEST(0.05,
        CASE
            WHEN roof_age <= 5 THEN UNIFORM(0.80::FLOAT, 0.99::FLOAT, RANDOM())
            WHEN roof_age <= 15 THEN UNIFORM(0.50::FLOAT, 0.85::FLOAT, RANDOM())
            WHEN roof_age <= 25 THEN UNIFORM(0.25::FLOAT, 0.55::FLOAT, RANDOM())
            ELSE UNIFORM(0.05::FLOAT, 0.35::FLOAT, RANDOM())
        END
    )), 2) AS roof_condition_score,
    CASE
        WHEN property_type = 'Commercial' THEN
            CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'TPO' WHEN 2 THEN 'EPDM' WHEN 3 THEN 'Metal' ELSE 'Built-Up' END
        WHEN year_built >= 2000 THEN
            CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'Architectural Shingle' WHEN 2 THEN 'Metal' WHEN 3 THEN 'Tile' ELSE 'Composite' END
        ELSE
            CASE UNIFORM(1, 4, RANDOM()) WHEN 1 THEN 'Asphalt Shingle' WHEN 2 THEN 'Wood Shake' WHEN 3 THEN 'Slate' ELSE 'Asphalt Shingle' END
    END AS roof_material,
    ROUND(LEAST(1.0, GREATEST(0.02,
        CASE
            WHEN year_built >= 2000 THEN UNIFORM(0.05::FLOAT, 0.30::FLOAT, RANDOM())
            WHEN year_built >= 1980 THEN UNIFORM(0.15::FLOAT, 0.50::FLOAT, RANDOM())
            WHEN year_built >= 1960 THEN UNIFORM(0.30::FLOAT, 0.70::FLOAT, RANDOM())
            ELSE UNIFORM(0.45::FLOAT, 0.90::FLOAT, RANDOM())
        END +
        CASE construction_type
            WHEN 'Concrete' THEN -0.10 WHEN 'Steel' THEN -0.08 WHEN 'Masonry' THEN -0.05 WHEN 'Mixed' THEN 0.0 ELSE 0.05
        END
    )), 2) AS structural_risk_score,
    CASE
        WHEN region = 'wildfire' THEN
            CASE WHEN veg_pct <= 10 THEN 'Low' WHEN veg_pct <= 35 THEN 'Medium' ELSE 'High' END
        WHEN region = 'arid' THEN
            CASE WHEN veg_pct <= 65 THEN 'Low' WHEN veg_pct <= 90 THEN 'Medium' ELSE 'High' END
        WHEN property_type = 'Commercial' THEN
            CASE WHEN veg_pct <= 50 THEN 'Low' WHEN veg_pct <= 80 THEN 'Medium' ELSE 'High' END
        ELSE
            CASE WHEN veg_pct <= 33 THEN 'Low' WHEN veg_pct <= 66 THEN 'Medium' ELSE 'High' END
    END AS vegetation_density
FROM base;

-- -----------------------------------------------------------------------------
-- 3. RISK_GEOSPATIAL (10,000 rows)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.RISK_GEOSPATIAL AS
WITH region_map AS (
    SELECT column1 AS city, column2 AS region_type
    FROM VALUES
        ('Miami','coastal'),('Tampa','coastal'),('Jacksonville','coastal'),
        ('Houston','coastal'),('Galveston','coastal'),('New Orleans','coastal'),
        ('Charleston','coastal'),('Savannah','coastal'),('Norfolk','coastal'),
        ('San Diego','coastal'),('Los Angeles','wildfire'),('San Francisco','coastal'),
        ('Sacramento','wildfire'),('Portland','inland'),('Seattle','coastal'),
        ('Phoenix','arid'),('Denver','inland'),('Dallas','tornado'),
        ('Oklahoma City','tornado'),('Kansas City','tornado'),
        ('Chicago','inland'),('Detroit','inland'),('Minneapolis','inland'),
        ('Atlanta','inland'),('Nashville','inland'),('Charlotte','inland'),
        ('Boston','coastal'),('New York','coastal'),('Philadelphia','inland'),
        ('Baltimore','coastal')
),
base AS (
    SELECT
        p.property_id,
        COALESCE(rm.region_type, 'inland') AS region,
        UNIFORM(1, 100, RANDOM()) AS flood_pct,
        UNIFORM(1, 100, RANDOM()) AS wildfire_pct,
        UNIFORM(1, 100, RANDOM()) AS crime_pct
    FROM HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER p
    LEFT JOIN region_map rm ON p.city = rm.city
),
risk_vals AS (
    SELECT
        property_id,
        region,
        CASE
            WHEN region = 'coastal' THEN
                CASE WHEN flood_pct <= 40 THEN 'High' WHEN flood_pct <= 75 THEN 'Medium' ELSE 'Low' END
            WHEN region = 'tornado' THEN
                CASE WHEN flood_pct <= 25 THEN 'High' WHEN flood_pct <= 60 THEN 'Medium' ELSE 'Low' END
            WHEN region = 'arid' THEN
                CASE WHEN flood_pct <= 5 THEN 'High' WHEN flood_pct <= 20 THEN 'Medium' ELSE 'Low' END
            ELSE
                CASE WHEN flood_pct <= 10 THEN 'High' WHEN flood_pct <= 35 THEN 'Medium' ELSE 'Low' END
        END AS flood_zone,
        CASE
            WHEN region = 'wildfire' THEN
                CASE WHEN wildfire_pct <= 45 THEN 'High' WHEN wildfire_pct <= 80 THEN 'Medium' ELSE 'Low' END
            WHEN region = 'arid' THEN
                CASE WHEN wildfire_pct <= 30 THEN 'High' WHEN wildfire_pct <= 65 THEN 'Medium' ELSE 'Low' END
            WHEN region = 'coastal' THEN
                CASE WHEN wildfire_pct <= 8 THEN 'High' WHEN wildfire_pct <= 25 THEN 'Medium' ELSE 'Low' END
            ELSE
                CASE WHEN wildfire_pct <= 5 THEN 'High' WHEN wildfire_pct <= 20 THEN 'Medium' ELSE 'Low' END
        END AS wildfire_risk,
        CASE
            WHEN region IN ('coastal','inland') THEN UNIFORM(20, 85, RANDOM())
            WHEN region = 'tornado' THEN UNIFORM(15, 70, RANDOM())
            ELSE UNIFORM(10, 60, RANDOM())
        END AS crime_index,
        CASE
            WHEN region = 'coastal' THEN ROUND(UNIFORM(0.5::FLOAT, 20.0::FLOAT, RANDOM()), 1)
            WHEN region = 'wildfire' THEN ROUND(UNIFORM(30.0::FLOAT, 150.0::FLOAT, RANDOM()), 1)
            WHEN region = 'tornado' THEN ROUND(UNIFORM(200.0::FLOAT, 800.0::FLOAT, RANDOM()), 1)
            WHEN region = 'arid' THEN ROUND(UNIFORM(300.0::FLOAT, 900.0::FLOAT, RANDOM()), 1)
            ELSE ROUND(UNIFORM(50.0::FLOAT, 500.0::FLOAT, RANDOM()), 1)
        END AS distance_to_coast_km
    FROM base
)
SELECT
    property_id,
    flood_zone,
    wildfire_risk,
    crime_index,
    distance_to_coast_km,
    ROUND(LEAST(1.0, GREATEST(0.0,
        0.35 * CASE flood_zone WHEN 'High' THEN 0.90 WHEN 'Medium' THEN 0.50 ELSE 0.15 END +
        0.30 * CASE wildfire_risk WHEN 'High' THEN 0.90 WHEN 'Medium' THEN 0.50 ELSE 0.15 END +
        0.20 * crime_index / 100.0 +
        0.15 * LEAST(1.0, GREATEST(0.0, 1.0 - distance_to_coast_km / 100.0)) +
        UNIFORM(-0.05::FLOAT, 0.05::FLOAT, RANDOM())
    )), 3) AS hazard_score
FROM risk_vals;

-- -----------------------------------------------------------------------------
-- 4. WEATHER_HISTORY (10,000 rows)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.WEATHER_HISTORY AS
WITH region_map AS (
    SELECT column1 AS city, column2 AS region_type
    FROM VALUES
        ('Miami','coastal'),('Tampa','coastal'),('Jacksonville','coastal'),
        ('Houston','coastal'),('Galveston','coastal'),('New Orleans','coastal'),
        ('Charleston','coastal'),('Savannah','coastal'),('Norfolk','coastal'),
        ('San Diego','coastal'),('Los Angeles','wildfire'),('San Francisco','coastal'),
        ('Sacramento','wildfire'),('Portland','inland'),('Seattle','coastal'),
        ('Phoenix','arid'),('Denver','inland'),('Dallas','tornado'),
        ('Oklahoma City','tornado'),('Kansas City','tornado'),
        ('Chicago','inland'),('Detroit','inland'),('Minneapolis','inland'),
        ('Atlanta','inland'),('Nashville','inland'),('Charlotte','inland'),
        ('Boston','coastal'),('New York','coastal'),('Philadelphia','inland'),
        ('Baltimore','coastal')
)
SELECT
    p.property_id,
    ROUND(CASE
        WHEN rm.region_type = 'coastal' AND p.state IN ('FL','TX','LA') THEN UNIFORM(45.0::FLOAT, 65.0::FLOAT, RANDOM())
        WHEN rm.region_type = 'coastal' THEN UNIFORM(35.0::FLOAT, 55.0::FLOAT, RANDOM())
        WHEN rm.region_type = 'wildfire' THEN UNIFORM(10.0::FLOAT, 25.0::FLOAT, RANDOM())
        WHEN rm.region_type = 'arid' THEN UNIFORM(5.0::FLOAT, 15.0::FLOAT, RANDOM())
        WHEN rm.region_type = 'tornado' THEN UNIFORM(30.0::FLOAT, 50.0::FLOAT, RANDOM())
        ELSE UNIFORM(25.0::FLOAT, 45.0::FLOAT, RANDOM())
    END, 1) AS avg_annual_rainfall,
    CASE
        WHEN rm.region_type = 'coastal' AND p.state IN ('FL','TX','LA') THEN UNIFORM(8, 18, RANDOM())
        WHEN rm.region_type = 'coastal' THEN UNIFORM(5, 12, RANDOM())
        WHEN rm.region_type = 'tornado' THEN UNIFORM(8, 20, RANDOM())
        WHEN rm.region_type = 'wildfire' THEN UNIFORM(2, 8, RANDOM())
        WHEN rm.region_type = 'arid' THEN UNIFORM(1, 5, RANDOM())
        ELSE UNIFORM(3, 10, RANDOM())
    END AS storm_frequency,
    ROUND(CASE
        WHEN p.state IN ('FL','TX','LA','GA','SC') THEN UNIFORM(68.0::FLOAT, 82.0::FLOAT, RANDOM())
        WHEN p.state IN ('CA','AZ') THEN UNIFORM(60.0::FLOAT, 90.0::FLOAT, RANDOM())
        WHEN p.state IN ('MN','MI','IL') THEN UNIFORM(40.0::FLOAT, 55.0::FLOAT, RANDOM())
        WHEN p.state IN ('CO','OR','WA') THEN UNIFORM(45.0::FLOAT, 60.0::FLOAT, RANDOM())
        ELSE UNIFORM(50.0::FLOAT, 65.0::FLOAT, RANDOM())
    END, 1) AS avg_temperature,
    CASE
        WHEN rm.region_type = 'coastal' AND p.state IN ('FL','TX','LA') THEN UNIFORM(2, 8, RANDOM())
        WHEN rm.region_type = 'coastal' AND p.state IN ('SC','GA','VA','NC','MD') THEN UNIFORM(1, 5, RANDOM())
        WHEN rm.region_type = 'coastal' AND p.state IN ('MA','NY') THEN UNIFORM(0, 3, RANDOM())
        WHEN rm.region_type = 'coastal' THEN UNIFORM(0, 2, RANDOM())
        ELSE UNIFORM(0, 1, RANDOM())
    END AS hurricane_events_last_10y
FROM HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER p
LEFT JOIN region_map rm ON p.city = rm.city;

-- -----------------------------------------------------------------------------
-- 5. PREMIUM_BENCHMARK (10,000 rows)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.PREMIUM_BENCHMARK AS
WITH base AS (
    SELECT
        p.property_id,
        CASE
            WHEN p.insured_value <= 500000 THEN p.insured_value * 0.005
            WHEN p.insured_value <= 2000000 THEN p.insured_value * 0.004
            ELSE p.insured_value * 0.003
        END AS base_prem,
        r.hazard_score,
        UNIFORM(-500::FLOAT, 500::FLOAT, RANDOM()) AS noise
    FROM HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER p
    JOIN HITL_SBA_DB.PC_INSURANCE.RISK_GEOSPATIAL r ON p.property_id = r.property_id
),
computed AS (
    SELECT
        property_id,
        GREATEST(base_prem * (1 + hazard_score * 2.0) + noise, base_prem * 0.5) AS avg_prem
    FROM base
)
SELECT
    property_id,
    ROUND(avg_prem, 2) AS avg_market_premium,
    ROUND(avg_prem * UNIFORM(0.65::FLOAT, 0.80::FLOAT, RANDOM()), 2) AS min_premium,
    ROUND(avg_prem * UNIFORM(1.25::FLOAT, 1.50::FLOAT, RANDOM()), 2) AS max_premium
FROM computed;

-- -----------------------------------------------------------------------------
-- 6. CLAIMS_HISTORY (~30,000 rows)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.CLAIMS_HISTORY AS
WITH ranked AS (
    SELECT
        p.property_id,
        p.insured_value,
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
    FROM HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER p
),
property_tiers AS (
    SELECT
        property_id,
        insured_value,
        CASE
            WHEN rn <= 3000 THEN 0
            WHEN rn <= 7000 THEN UNIFORM(1, 3, RANDOM())
            WHEN rn <= 9000 THEN UNIFORM(3, 6, RANDOM())
            ELSE UNIFORM(8, 18, RANDOM())
        END AS num_claims
    FROM ranked
),
expanded AS (
    SELECT
        pt.property_id,
        pt.insured_value,
        f.VALUE::INT AS claim_idx
    FROM property_tiers pt,
    LATERAL FLATTEN(ARRAY_GENERATE_RANGE(0, GREATEST(pt.num_claims, 0))) f
    WHERE pt.num_claims > 0
),
enriched AS (
    SELECT
        e.property_id,
        e.insured_value,
        e.claim_idx,
        r.flood_zone,
        r.wildfire_risk,
        r.crime_index,
        UNIFORM(1, 100, RANDOM()) AS type_pct,
        UNIFORM(1, 100, RANDOM()) AS severity_pct,
        UNIFORM(2010, 2025, RANDOM()) AS claim_yr
    FROM expanded e
    JOIN HITL_SBA_DB.PC_INSURANCE.RISK_GEOSPATIAL r ON e.property_id = r.property_id
)
SELECT
    'CLM-' || LPAD(ROW_NUMBER() OVER (ORDER BY property_id, claim_idx)::STRING, 6, '0') AS claim_id,
    property_id,
    claim_yr AS claim_year,
    CASE
        WHEN severity_pct <= 50 THEN UNIFORM(500, 15000, RANDOM())
        WHEN severity_pct <= 85 THEN UNIFORM(15000, 75000, RANDOM())
        ELSE UNIFORM(75000, 500000, RANDOM())
    END AS claim_amount,
    CASE
        WHEN flood_zone = 'High' THEN
            CASE WHEN type_pct <= 45 THEN 'Flood' WHEN type_pct <= 70 THEN 'Storm' WHEN type_pct <= 85 THEN 'Fire' ELSE 'Theft' END
        WHEN wildfire_risk = 'High' THEN
            CASE WHEN type_pct <= 45 THEN 'Fire' WHEN type_pct <= 70 THEN 'Storm' WHEN type_pct <= 85 THEN 'Flood' ELSE 'Theft' END
        WHEN crime_index > 70 THEN
            CASE WHEN type_pct <= 40 THEN 'Theft' WHEN type_pct <= 65 THEN 'Storm' WHEN type_pct <= 85 THEN 'Flood' ELSE 'Fire' END
        ELSE
            CASE WHEN type_pct <= 30 THEN 'Storm' WHEN type_pct <= 55 THEN 'Flood' WHEN type_pct <= 80 THEN 'Fire' ELSE 'Theft' END
    END AS claim_type,
    CASE
        WHEN severity_pct <= 50 THEN 'Low'
        WHEN severity_pct <= 85 THEN 'Medium'
        ELSE 'High'
    END AS claim_severity
FROM enriched;

-- -----------------------------------------------------------------------------
-- 7. MODEL_OUTPUT (10,000 rows) — derived from all upstream tables
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE HITL_SBA_DB.PC_INSURANCE.MODEL_OUTPUT AS
WITH claims_agg AS (
    SELECT
        property_id,
        COUNT(*) AS total_claims,
        AVG(claim_amount) AS avg_claim_amt,
        SUM(CASE WHEN claim_severity = 'High' THEN 1 ELSE 0 END) AS high_severity_count
    FROM HITL_SBA_DB.PC_INSURANCE.CLAIMS_HISTORY
    GROUP BY property_id
),
raw_scores AS (
    SELECT
        p.property_id,
        p.insured_value,
        r.hazard_score,
        r.crime_index,
        w.storm_frequency,
        w.hurricane_events_last_10y,
        a.roof_condition_score,
        a.structural_risk_score,
        COALESCE(c.total_claims, 0) AS total_claims,
        COALESCE(c.avg_claim_amt, 0) AS avg_claim_amt,
        ROUND(LEAST(1.0, GREATEST(0.0,
            0.20 * r.hazard_score +
            0.15 * a.structural_risk_score +
            0.12 * (1.0 - a.roof_condition_score) +
            0.10 * LEAST(w.storm_frequency / 15.0, 1.0) +
            0.08 * LEAST(w.hurricane_events_last_10y / 5.0, 1.0) +
            0.15 * LEAST(COALESCE(c.total_claims, 0) / 10.0, 1.0) +
            0.10 * LEAST(COALESCE(c.avg_claim_amt, 0) / 200000.0, 1.0) +
            0.05 * r.crime_index / 100.0 +
            0.05 * UNIFORM(-0.1::FLOAT, 0.1::FLOAT, RANDOM())
        )), 3) AS risk_score
    FROM HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER p
    JOIN HITL_SBA_DB.PC_INSURANCE.RISK_GEOSPATIAL r ON p.property_id = r.property_id
    JOIN HITL_SBA_DB.PC_INSURANCE.WEATHER_HISTORY w ON p.property_id = w.property_id
    JOIN HITL_SBA_DB.PC_INSURANCE.AERIAL_ANALYSIS a ON p.property_id = a.property_id
    LEFT JOIN claims_agg c ON p.property_id = c.property_id
),
scored AS (
    SELECT *,
        NTILE(10) OVER (ORDER BY risk_score) AS score_decile
    FROM raw_scores
),
categorized AS (
    SELECT *,
        CASE
            WHEN score_decile <= 3 THEN 'Low'
            WHEN score_decile <= 8 THEN 'Medium'
            ELSE 'High'
        END AS risk_category
    FROM scored
)
SELECT
    property_id,
    risk_score,
    risk_category,
    CASE
        WHEN risk_category = 'High' AND UNIFORM(1, 100, RANDOM()) <= 70 THEN 'Decline'
        WHEN risk_category = 'Medium' AND UNIFORM(1, 100, RANDOM()) <= 10 THEN 'Decline'
        ELSE 'Write'
    END AS recommended_action,
    ROUND(
        insured_value * 0.005 * (1.0 + risk_score * 3.0) *
        CASE risk_category
            WHEN 'High' THEN UNIFORM(1.2::FLOAT, 1.5::FLOAT, RANDOM())
            WHEN 'Medium' THEN UNIFORM(0.9::FLOAT, 1.1::FLOAT, RANDOM())
            ELSE UNIFORM(0.7::FLOAT, 0.9::FLOAT, RANDOM())
        END,
    2) AS suggested_premium
FROM categorized;

-- -----------------------------------------------------------------------------
-- 8. Internal stages for Image Analysis & AI Assistant tabs
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS HITL_SBA_DB.PC_INSURANCE.IMAGE_ANALYSIS_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE STAGE IF NOT EXISTS HITL_SBA_DB.PC_INSURANCE.IMAGE_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
