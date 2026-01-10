with
-- Retrieve columns only from the current database
src_columns as (select * from source.information_schema.columns where table_catalog = current_database()),

-- Retrieve tables only from the current database
src_tables as (select * from source.information_schema.tables where table_catalog = current_database()),

stg_timezone_mapping as (select * from STAGING.Public.TIMEZONE_MAPPING_DATA),

 

column_length as (

 

    select 

        table_name,

        max(len(column_name)) as columnlength

    from src_columns 

 

    group by table_name

),

 

staging_columns as (

 

    select distinct 

        c.*,

        table_tz.data_type,

        table_tz.time_zone,

        schema_tz.data_type,

        schema_tz.time_zone,

        case

            when c.Data_type='TIMESTAMP_TZ'

            then rpad(concat('\{\{ convert_timezone_format(\'', c.column_name, '\',\'', coalesce(table_tz.Data_Type, schema_tz.Data_Type, c.Data_Type,'Need Timezone Record'),'\') }}'), cl.columnlength+50, ' ') ||' as ' || lower(c.column_name)

            when c.Data_type='TIMESTAMP_NTZ'

            then rpad(concat('\{\{ convert_timezone_format(\'',c.column_name,'\',\'',coalesce(table_tz.Data_Type, schema_tz.Data_Type, c.Data_Type, 'Need Timezone Record'),'\',\'',coalesce(table_tz.Time_Zone, schema_tz.Time_Zone, 'Need Timezone Record'),'\') }}'), cl.columnlength+50, ' ') ||' as ' || lower(c.column_name)

            else rpad(c.column_name, cl.columnlength+50, ' ') ||' as ' || lower(c.column_name)

        end as staging_column

 

    from src_columns               as c

    join column_length             as cl    on cl.table_name = c.table_name

    left join stg_timezone_mapping as table_tz on table_tz.table_schema=c.table_schema and c.table_name=table_tz.table_name and c.column_name=table_tz.Column_name

    left join stg_timezone_mapping as schema_tz on schema_tz.table_schema=c.table_schema and schema_tz.data_type = c.data_type

),

 

final as (

 

    select CONCAT(

      'with',

      char(10),

      'src_',

      lower(t.table_name) ,

      ' as (select * from \{\{ source(\'' ,

      lower(t.table_schema), 

      '\', \'',

      t.table_name,

      '\', ', 

      1, 

      ') }} ),',

      char(10),

      char(10),

      'final as (',

      char(10),

      '    select',

      char(10),

      '        ',

      (select  listagg( sc.staging_column , (',\n        ')) within group (order by ordinal_position) from staging_columns sc where sc.table_schema = t.table_schema and sc.table_name = t.table_name),

 

      char(10),

 

      '    from ',

 

      'src_',

 

      lower(t.table_name) , 

 

      char(10),

 

      ')',

 

      char(10),

 

      char(10),

 

      'select * from final'

 

    )

 

    from src_tables t

    where t.table_schema = upper('INTRANETCONTENT_INTRANETPROXY')  -- put schema name here

    and t.table_type = 'BASE TABLE' 

    and t.table_name = upper('EMPLOYEE')

 

    order by t.table_name

)

 

select * from final