CREATE OR REPLACE TABLE KE_RAW_JSON (
    file_name STRING,
    s3_last_modified TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data VARIANT
);

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = JSON;

COPY INTO KE_RAW_JSON
    (file_name, s3_last_modified, load_timestamp, raw_data)
FROM (
    SELECT
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        CURRENT_TIMESTAMP(),
        $1
    FROM @STAGE_KE_RAW
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';



LIST @STAGE_KE_RAW;


SELECT FILE_NAME, S3_LAST_MODIFIED,LOAD_TIMESTAMP, RAW_DATA FROM KE_RAW_JSON ;


-----------------< TASK_CREATION >----------------------------


SELECT
    METADATA$FILENAME,
    COUNT(*)
FROM @STAGE_KE_RAW
GROUP BY METADATA$FILENAME
HAVING COUNT(*) > 1;

CREATE OR REPLACE TASK KE_COPY_FROM_STAGE_TASK
WAREHOUSE = 'WH_LARGE_SNOWPARK_WAREHOUSE'
SCHEDULE = '5 MINUTE'
AS

MERGE INTO KE_RAW_JSON T
USING (
    SELECT *
    FROM (
        SELECT
            METADATA$FILENAME           AS file_name,
            METADATA$FILE_LAST_MODIFIED AS s3_last_modified,
            CURRENT_TIMESTAMP()         AS load_timestamp,
            $1                          AS raw_data,
            ROW_NUMBER() OVER (
                PARTITION BY METADATA$FILENAME
                ORDER BY METADATA$FILE_LAST_MODIFIED DESC
            ) AS rn
        FROM @STAGE_KE_RAW (FILE_FORMAT => 'JSON_FORMAT')
        WHERE METADATA$FILE_LAST_MODIFIED
              >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    )
    WHERE rn = 1
) S
ON T.file_name = S.file_name

WHEN MATCHED
     AND T.s3_last_modified <> S.s3_last_modified THEN
    UPDATE SET
        s3_last_modified = S.s3_last_modified,
        load_timestamp   = S.load_timestamp,
        raw_data         = S.raw_data

WHEN NOT MATCHED THEN
    INSERT (file_name, s3_last_modified, load_timestamp, raw_data)
    VALUES (S.file_name, S.s3_last_modified, S.load_timestamp, S.raw_data);


ALTER TASK KE_COPY_FROM_STAGE_TASK RESUME;


-----------------< TABLE_CREATION >----------------------------

CREATE OR REPLACE TABLE KE_DATA (
    registration_number STRING NOT NULL,

    business_id NUMBER,
    business_type_slug STRING,
    name STRING,
    email STRING,
    phone_number STRING,
    registration_date DATE,
    status STRING,
    verified BOOLEAN,

    entity_id NUMBER,
    entity_type STRING,
    dataset_id NUMBER,
    entity_inserted_at TIMESTAMP,
    entity_updated_at TIMESTAMP,
    entity_uuid STRING,

    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),  -- warehouse insert time
    updated_at TIMESTAMP,                               -- warehouse update time
    raw_load_timestamp TIMESTAMP,                       -- when raw file loaded

    CONSTRAINT PK_REG PRIMARY KEY (registration_number)
);


-----------------< LOAD_MANUALLY_FIRST >----------------------------


INSERT INTO KE_DATA (
    registration_number,
    business_id,
    business_type_slug,
    name,
    email,
    phone_number,
    registration_date,
    status,
    verified,
    entity_id,
    entity_type,
    dataset_id,
    entity_inserted_at,
    entity_updated_at,
    entity_uuid,
    inserted_at,
    raw_load_timestamp
)
SELECT
    RAW_DATA:registration_number::STRING,
    RAW_DATA:id::NUMBER,
    RAW_DATA:business_type_slug::STRING,
    RAW_DATA:name::STRING,
    RAW_DATA:email::STRING,
    RAW_DATA:phone_number::STRING,
    RAW_DATA:registration_date::DATE,
    RAW_DATA:status::STRING,
    RAW_DATA:verified::BOOLEAN,
    RAW_DATA:entity:id::NUMBER,
    RAW_DATA:entity:entity_type::STRING,
    RAW_DATA:entity:dataset_id::NUMBER,
    RAW_DATA:entity:inserted_at::TIMESTAMP,
    RAW_DATA:entity:updated_at::TIMESTAMP,
    RAW_DATA:entity:uuid::STRING,
    CURRENT_TIMESTAMP(),
    LOAD_TIMESTAMP
FROM KE_RAW_JSON
WHERE RAW_DATA:registration_number IS NOT NULL;

SELECT * FROM KE_DATA ;


-----------------< STREAM_CREATION >----------------------------

CREATE OR REPLACE STREAM KE_RAW_JSON_STREAM
ON TABLE KE_RAW_JSON;

SELECT * FROM KE_RAW_JSON_STREAM;

-----------------< EVENT_MERGE_TASK >----------------------------


CREATE OR REPLACE TASK KE_RAW_TO_DATA_TASK
WAREHOUSE = 'WH_LARGE_SNOWPARK_WAREHOUSE'
WHEN SYSTEM$STREAM_HAS_DATA('KE_RAW_JSON_STREAM')
AS

MERGE INTO KE_DATA T
USING (
    SELECT
        raw_data:registration_number::STRING       AS registration_number,
        raw_data:id::NUMBER                        AS business_id,
        raw_data:business_type_slug::STRING        AS business_type_slug,
        raw_data:name::STRING                      AS name,
        raw_data:email::STRING                     AS email,
        raw_data:phone_number::STRING              AS phone_number,
        raw_data:registration_date::DATE           AS registration_date,
        raw_data:status::STRING                    AS status,
        raw_data:verified::BOOLEAN                 AS verified,

        raw_data:entity:id::NUMBER                 AS entity_id,
        raw_data:entity:entity_type::STRING        AS entity_type,
        raw_data:entity:dataset_id::NUMBER         AS dataset_id,
        raw_data:entity:inserted_at::TIMESTAMP     AS entity_inserted_at,
        raw_data:entity:updated_at::TIMESTAMP      AS entity_updated_at,
        raw_data:entity:uuid::STRING               AS entity_uuid,

        CURRENT_TIMESTAMP()                        AS load_time

    FROM KE_RAW_JSON_STREAM
    WHERE METADATA$ACTION = 'INSERT'
) S

ON T.registration_number = S.registration_number


WHEN MATCHED AND (
       NVL(T.business_id, -1) <> NVL(S.business_id, -1)
    OR NVL(T.business_type_slug, 'X') <> NVL(S.business_type_slug, 'X')
    OR NVL(T.name, 'X') <> NVL(S.name, 'X')
    OR NVL(T.email, 'X') <> NVL(S.email, 'X')
    OR NVL(T.phone_number, 'X') <> NVL(S.phone_number, 'X')
    OR NVL(T.registration_date, '1900-01-01'::DATE)
         <> NVL(S.registration_date, '1900-01-01'::DATE)
    OR NVL(T.status, 'X') <> NVL(S.status, 'X')
    OR NVL(T.verified, FALSE) <> NVL(S.verified, FALSE)
    OR NVL(T.entity_id, -1) <> NVL(S.entity_id, -1)
    OR NVL(T.entity_type, 'X') <> NVL(S.entity_type, 'X')
    OR NVL(T.dataset_id, -1) <> NVL(S.dataset_id, -1)
    OR NVL(T.entity_inserted_at, '1900-01-01'::TIMESTAMP)
         <> NVL(S.entity_inserted_at, '1900-01-01'::TIMESTAMP)
    OR NVL(T.entity_updated_at, '1900-01-01'::TIMESTAMP)
         <> NVL(S.entity_updated_at, '1900-01-01'::TIMESTAMP)
)

THEN UPDATE SET
    T.business_id         = S.business_id,
    T.business_type_slug  = S.business_type_slug,
    T.name                = S.name,
    T.email               = S.email,
    T.phone_number        = S.phone_number,
    T.registration_date   = S.registration_date,
    T.status              = S.status,
    T.verified            = S.verified,
    T.entity_id           = S.entity_id,
    T.entity_type         = S.entity_type,
    T.dataset_id          = S.dataset_id,
    T.entity_inserted_at  = S.entity_inserted_at,
    T.entity_updated_at   = S.entity_updated_at,
    T.updated_at          = S.load_time,
    T.raw_load_timestamp  = S.load_time


WHEN NOT MATCHED THEN
    INSERT (
        registration_number,
        business_id,
        business_type_slug,
        name,
        email,
        phone_number,
        registration_date,
        status,
        verified,
        entity_id,
        entity_type,
        dataset_id,
        entity_inserted_at,
        entity_updated_at,
        inserted_at,
        updated_at,
        raw_load_timestamp
    )
    VALUES (
        S.registration_number,
        S.business_id,
        S.business_type_slug,
        S.name,
        S.email,
        S.phone_number,
        S.registration_date,
        S.status,
        S.verified,
        S.entity_id,
        S.entity_type,
        S.dataset_id,
        S.entity_inserted_at,
        S.entity_updated_at,
        S.load_time,     -- inserted_at (fixed forever)
        NULL,            -- updated_at initially null
        S.load_time
    );


ALTER TASK KE_RAW_TO_DATA_TASK RESUME;


SELECT * FROM KE_DATA
WHERE UPDATED_AT IS NOT NULL;



-----------------< MONITOR >----------------------------




SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC;



-----------------< Transfer_Table >----------------------------

CREATE OR REPLACE STREAM KE_DATA_STREAM
ON TABLE sample_database.PUBLIC.KE_DATA;



CREATE OR REPLACE TASK KE_DATA_TO_ANALYSIS_TASK
WAREHOUSE = 'WH_LARGE_SNOWPARK_WAREHOUSE'
WHEN SYSTEM$STREAM_HAS_DATA('sample_database.PUBLIC.KE_DATA_STREAM')
AS

MERGE INTO sample_countries.PUBLIC.KE_ANALYSIS T
USING (
    SELECT *
    FROM sample_database.PUBLIC.KE_DATA_STREAM
    WHERE METADATA$ACTION = 'INSERT'
) S

ON T.registration_number = S.registration_number

WHEN MATCHED THEN
    UPDATE SET
        business_id        = S.business_id,
        business_type_slug = S.business_type_slug,
        name               = S.name,
        email              = S.email,
        phone_number       = S.phone_number,
        registration_date  = S.registration_date,
        status             = S.status,
        verified           = S.verified,
        entity_id          = S.entity_id,
        entity_type        = S.entity_type,
        dataset_id         = S.dataset_id,
        entity_inserted_at = S.entity_inserted_at,
        entity_updated_at  = S.entity_updated_at,
        entity_uuid        = S.entity_uuid,
        inserted_at        = S.inserted_at,
        updated_at         = S.updated_at,
        raw_load_timestamp = S.raw_load_timestamp

WHEN NOT MATCHED THEN
    INSERT (
        registration_number,
        business_id,
        business_type_slug,
        name,
        email,
        phone_number,
        registration_date,
        status,
        verified,
        entity_id,
        entity_type,
        dataset_id,
        entity_inserted_at,
        entity_updated_at,
        entity_uuid,
        inserted_at,
        updated_at,
        raw_load_timestamp
    )
    VALUES (
        S.registration_number,
        S.business_id,
        S.business_type_slug,
        S.name,
        S.email,
        S.phone_number,
        S.registration_date,
        S.status,
        S.verified,
        S.entity_id,
        S.entity_type,
        S.dataset_id,
        S.entity_inserted_at,
        S.entity_updated_at,
        S.entity_uuid,
        S.inserted_at,
        S.updated_at,
        S.raw_load_timestamp
    );


ALTER TASK KE_DATA_TO_ANALYSIS_TASK RESUME;



-----------------< CLEAN >----------------------------

ALTER TASK IF EXISTS KE_COPY_FROM_STAGE_TASK SUSPEND;
ALTER TASK IF EXISTS KE_RAW_TO_DATA_TASK SUSPEND;

DROP STREAM IF EXISTS KE_RAW_JSON_STREAM;
DROP STREAM IF EXISTS  KE_DATA_STREAM ;

ALTER TASK KE_DATA_TO_ANALYSIS_TASK SUSPEND;


-----------------< DELETE >----------------------------

DROP TASK IF EXISTS KE_COPY_FROM_STAGE_TASK;
DROP TASK IF EXISTS KE_RAW_TO_DATA_TASK;
DROP TASK IF EXISTS KE_DATA_TO_ANALYSIS_TASK;