-- with recursive CTE to build a data structure of words
with recursive
-- Work bank - has words to be capitalized in pascal case
word_bank(word, priority) as (
    select word, row_number() over (order by len(word) desc) as priority
    from (values 
        ('Id'), ('By'), ('Name'), ('On'), ('To'), ('From'), 
        ('Synced'), ('Deleted'), ('Project'), ('Sponsor'), 
        ('Created'), ('Updated'), ('Valid'), ('Closed'), ('Protocol')
    ) as t(word)
),
with
-- Retrieve columns only from the current database
src_columns as (select * from source.information_schema.columns where table_catalog = current_database()),

-- Retrieve tables only from the current database
src_tables as (select * from source.information_schema.tables where table_catalog = current_database()),

stg_timezone_mapping as (select * from STAGING.Public.TIMEZONE_MAPPING_DATA),

-- Loop through the word bank for every column
fix_names(column_name, table_name, current_name, step) as (
    -- Start with initcap version of the column name
    select column_name, table_name, initcap(column_name), 0
    from src_columns
    
    union all
    
    -- Recursively apply the next word from the bank
    select 
        f.column_name, 
        f.table_name, 
        replace(f.current_name, lower(wb.word), wb.word), 
        f.step + 1
    from fix_names f
    join word_bank wb on f.step + 1 = wb.priority
),

-- Get the final result of the recursive calls
transformed_names as (
    select column_name, table_name, replace(current_name, '_', '') as pascal_column
    from fix_names
    where step = (select max(priority) from word_bank)
),

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
        tn.pascal_column,
        case
            when c.Data_type='TIMESTAMP_TZ'
            -- You can remove the 'UTC' on the line below and replace it with another timezone if needed
            then rpad(concat('\{\{ convert_timezone_format(\'', c.column_name, '\',\'', coalesce(table_tz.Data_Type, schema_tz.Data_Type, c.Data_Type,'UTC'),'\') }}'), cl.columnlength+50, ' ') ||' as ' || tn.pascal_column

            when c.Data_type='TIMESTAMP_NTZ'
            -- You can remove the 2 'UTC' instances and replace them with different timezones if needed
            then rpad(concat('\{\{ convert_timezone_format(\'',c.column_name,'\',\'',coalesce(table_tz.Data_Type, schema_tz.Data_Type, c.Data_Type, 'UTC'),'\',\'',coalesce(table_tz.Time_Zone, schema_tz.Time_Zone, 'UTC'),'\') }}'), cl.columnlength+50, ' ') ||' as ' || tn.pascal_column

            else rpad(c.column_name, cl.columnlength+50, ' ') ||' as ' || tn.pascal_column
        end as staging_column

    from src_columns as c
    join transformed_names as tn on tn.column_name = c.column_name and tn.table_name = c.table_name
    join column_length as cl on cl.table_name = c.table_name
    left join stg_timezone_mapping as table_tz on table_tz.table_schema=c.table_schema and c.table_name=table_tz.table_name and c.column_name=table_tz.Column_name
    left join stg_timezone_mapping as schema_tz on schema_tz.table_schema=c.table_schema and schema_tz.data_type = c.data_type
),

final as (
    select CONCAT(
      'with',
      char(10),
      'src_',
      lower(t.table_name),
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