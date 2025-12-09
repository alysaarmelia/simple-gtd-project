@echo off
REM ==========================================
REM GTD Analytics Platform - DBT Setup Script (FIXED)
REM Run this INSIDE the 'simple-gtd-project' folder
REM ==========================================

echo Setting up dbt Configuration files...

REM --- Check if we are in the right place ---
if not exist "dbt_project" (
    echo ERROR: Could not find 'dbt_project' folder.
    echo Please make sure you are running this inside 'simple-gtd-project'.
    pause
    exit /b
)

cd dbt_project

REM ==========================================
REM 1. Create dbt_project.yml
REM ==========================================
echo Creating dbt_project.yml...
echo name: 'gtd_warehouse' > dbt_project.yml
echo version: '1.0.0' >> dbt_project.yml
echo config-version: 2 >> dbt_project.yml
echo. >> dbt_project.yml
echo profile: 'gtd_profile' >> dbt_project.yml
echo model-paths: ["models"] >> dbt_project.yml
echo seed-paths: ["seeds"] >> dbt_project.yml
echo test-paths: ["tests"] >> dbt_project.yml
echo analysis-paths: ["analyses"] >> dbt_project.yml
echo macro-paths: ["macros"] >> dbt_project.yml
echo. >> dbt_project.yml
echo target-path: "target" >> dbt_project.yml
echo clean-targets: ["target", "dbt_packages"] >> dbt_project.yml
echo. >> dbt_project.yml
echo models: >> dbt_project.yml
echo   gtd_warehouse: >> dbt_project.yml
echo     staging: >> dbt_project.yml
echo       +materialized: view >> dbt_project.yml
echo       +schema: staging >> dbt_project.yml
echo     warehouse: >> dbt_project.yml
echo       +materialized: table >> dbt_project.yml
echo       +schema: warehouse >> dbt_project.yml

REM ==========================================
REM 2. Create profiles.yml
REM ==========================================
echo Creating profiles.yml...
echo gtd_profile: > profiles.yml
echo   target: dev >> profiles.yml
echo   outputs: >> profiles.yml
echo     dev: >> profiles.yml
echo       type: postgres >> profiles.yml
echo       host: postgres >> profiles.yml
echo       user: airflow >> profiles.yml
echo       password: airflow >> profiles.yml
echo       port: 5432 >> profiles.yml
echo       dbname: airflow >> profiles.yml
echo       schema: public >> profiles.yml
echo       threads: 4 >> profiles.yml

REM ==========================================
REM 3. Create sources.yml
REM ==========================================
echo Creating models/staging/sources.yml...
echo version: 2 > models\staging\sources.yml
echo. >> models\staging\sources.yml
echo sources: >> models\staging\sources.yml
echo   - name: gtd_source >> models\staging\sources.yml
echo     database: airflow >> models\staging\sources.yml
echo     schema: public >> models\staging\sources.yml
echo     tables: >> models\staging\sources.yml
echo       - name: raw_gtd >> models\staging\sources.yml
echo         description: "Raw terrorist attack data loaded from MinIO" >> models\staging\sources.yml

REM ==========================================
REM 4. Create stg_attacks.sql
REM ==========================================
echo Creating models/staging/stg_attacks.sql...
REM We use careful escaping here for the SQL file
echo with raw_data as ( > models\staging\stg_attacks.sql
echo     select * from {{ source('gtd_source', 'raw_gtd') }} >> models\staging\stg_attacks.sql
echo ) >> models\staging\stg_attacks.sql
echo. >> models\staging\stg_attacks.sql
echo select >> models\staging\stg_attacks.sql
echo     eventid as event_id, >> models\staging\stg_attacks.sql
echo     iyear as year, >> models\staging\stg_attacks.sql
echo     imonth as month, >> models\staging\stg_attacks.sql
echo     iday as day, >> models\staging\stg_attacks.sql
echo     country_txt as country_name, >> models\staging\stg_attacks.sql
echo     region_txt as region_name, >> models\staging\stg_attacks.sql
echo     city as city_name, >> models\staging\stg_attacks.sql
echo     latitude, >> models\staging\stg_attacks.sql
echo     longitude, >> models\staging\stg_attacks.sql
echo     coalesce(nkill, 0) as killed, >> models\staging\stg_attacks.sql
echo     coalesce(nwound, 0) as wounded, >> models\staging\stg_attacks.sql
echo     attacktype1_txt as attack_type, >> models\staging\stg_attacks.sql
echo     targtype1_txt as target_type, >> models\staging\stg_attacks.sql
echo     gname as group_name, >> models\staging\stg_attacks.sql
echo     weaptype1_txt as weapon_type, >> models\staging\stg_attacks.sql
echo     summary >> models\staging\stg_attacks.sql
echo. >> models\staging\stg_attacks.sql
echo from raw_data >> models\staging\stg_attacks.sql
echo where iyear ^> 1900 >> models\staging\stg_attacks.sql

echo.
echo ==========================================
echo SUCCESS! dbt files created.
echo ==========================================
pause