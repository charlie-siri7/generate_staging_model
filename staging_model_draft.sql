with recursive
-- 1. WORD BANK: Centralized list for PascalCase logic.
word_bank(word, priority) as (
    select word, row_number() over (order by len(word) desc) as priority
    from (values 
        ('Id'), ('By'), ('Name'), ('On'), ('To'), ('From'), 
        ('Synced'), ('Deleted'), ('Project'), ('Sponsor'), 
        ('Created'), ('Updated'), ('Valid'), ('Closed'), ('Protocol'),
        ('Fivetran'), ('Study')
    ) as t(word)
),

-- Metadata retrieval limited to current database.
src_columns as (
    select * from source.information_schema.columns 
    where table_catalog = current_database() 
),

src_tables as (
    select * from source.information_schema.tables 
    where table_catalog = current_database() 
),

stg_timezone_mapping as (select * from STAGING.Public.TIMEZONE_MAPPING_DATA),

-- 2. RECURSIVE REPLACER: Handles the PascalCase transformation.
fix_names(column_name, table_name, current_name, step) as (
    select column_name, table_name, initcap(column_name), 0
    from src_columns
    
    union all
    
    select 
        f.column_name, 
        f.table_name, 
        replace(f.current_name, lower(wb.word), wb.word), 
        f.step + 1
    from fix_names f
    join word_bank wb on f.step + 1 = wb.priority
),

transformed_names as (
    select 
        column_name, 
        table_name, 
        replace(current_name, '_', '') as pascal_column
    from fix_names
    where step = (select max(priority) from word_bank)
),

-- NEW: Build the left-hand expression first so we can measure it.
prepared_expressions as (
    select 
        c.table_name,
        c.column_name,
        c.ordinal_position,
        c.table_schema,
        case
            when c.Data_type='TIMESTAMP_TZ'
            then concat('{{ convert_timezone_format(\'', c.column_name, '\',\'', coalesce(table_tz.Data_Type, schema_tz.Data_Type, c.Data_Type, 'UTC'),'\') }}')
            when c.Data_type='TIMESTAMP_NTZ'
            then concat('{{ convert_timezone_format(\'',c.column_name,'\',\'',coalesce(table_tz.Data_Type, schema_tz.Data_Type, c.Data_Type, 'UTC'),'\',\'',coalesce(table_tz.Time_Zone, schema_tz.Time_Zone, 'UTC'),'\') }}')
            else c.column_name
        end as left_side_expression
    from src_columns c
    left join stg_timezone_mapping as table_tz on table_tz.table_schema=c.table_schema and c.table_name=table_tz.table_name and c.column_name=table_tz.Column_name
    left join stg_timezone_mapping as schema_tz on schema_tz.table_schema=c.table_schema and schema_tz.data_type = c.data_type
),

-- CHANGED: Now we measure the ACTUAL length of the generated strings.
column_length as (
    select 
        table_name,
        max(len(left_side_expression)) as max_expression_len
    from prepared_expressions 
    group by table_name
),

staging_columns as (
    select distinct 
        pe.table_schema,
        pe.table_name,
        pe.ordinal_position,
        -- CHANGED: Use the actual measured length + 5 for a consistent gutter.
        rpad(pe.left_side_expression, cl.max_expression_len + 5, ' ') 
        || ' as ' || tn.pascal_column as staging_column
    from prepared_expressions pe
    join transformed_names tn on tn.column_name = pe.column_name and tn.table_name = pe.table_name
    join column_length cl on cl.table_name = pe.table_name
),

final as (
    select CONCAT(
      'with', char(10),
      'src_', lower(t.table_name), ' as (select * from {{ source(\'', lower(t.table_schema), '\', \'', t.table_name, '\', 1) }} ),',
      char(10), char(10),
      'final as (', char(10),
      '    select', char(10), '        ',
      (select listagg(sc.staging_column , (',\n        ')) within group (order by ordinal_position) from staging_columns sc where sc.table_schema = t.table_schema and sc.table_name = t.table_name),
      char(10), '    from ', 'src_', lower(t.table_name), char(10), ')',
      char(10), char(10), 'select * from final'
    )
    from src_tables t
    where t.table_schema = upper('MASTERDATA_IRTMASTERDATA')
    and t.table_type = 'BASE TABLE' 
    and t.table_name = upper('STUDY')
    order by t.table_name
)

select * from final;