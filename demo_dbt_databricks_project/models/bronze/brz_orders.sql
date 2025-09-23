select *,
       _metadata.file_modification_time AS file_modification_time,
       _metadata.file_name AS file_name,
       current_timestamp() as created_on
  from read_files(
      '/Volumes/demo_dbt_catalog/01_bronze/raw_data/raw_orders*.csv',
      format => 'csv',
      header => True);