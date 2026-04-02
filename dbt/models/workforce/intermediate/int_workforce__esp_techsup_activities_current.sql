{{

  config(

    materialized='table',

    alias='esp_techsup_activities',

    engine=ch_engine_merge_tree(),

    order_by='(dt, user_name, activity_type)',

    settings={'allow_nullable_key': 1},

    tags=['gs', 'esp_techsup_activities', 'current'],

  )

}}



with raw_source as (

  select *

  from {{ gs_bronze_parquet(

    'esp_techsup_activities',

    '`activity_date` Nullable(String), `user_name` Nullable(String), `activity_type` Nullable(String), `count_hours` Nullable(String), `ingested_at` Nullable(String)'

  ) }}

),

parsed as (

  select

    CAST(

      parseDateTime64BestEffortOrNull(

        nullIf(trimBoth(toString(`activity_date`)), ''),

        3

      ),

      'Nullable(DateTime64(3))'

    ) AS `dt`,

    CAST(`user_name`, 'Nullable(String)') AS `user_name`,

    CAST(`activity_type`, 'Nullable(String)') AS `activity_type`,

    CAST(toFloat32OrNull(replaceAll(nullIf(trimBoth(toString(`count_hours`)), ''), ',', '.')), 'Nullable(Float32)') AS `count_hours`,

    assumeNotNull(toDateTime64(parseDateTime64BestEffort('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}'), 3)) AS `ingested_at`

  from raw_source

)

select *

from parsed

where `dt` is not null

  and nullIf(trimBoth(toString(`user_name`)), '') is not null

