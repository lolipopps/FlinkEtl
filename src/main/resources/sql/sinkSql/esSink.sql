CREATE TABLE sink_es_shafa_pv_uv (
       ts  STRING,
       channel_id String,
       pv BIGINT,
       uv BIGINT
       ) WITH (
       'connector.type' = 'elasticsearch',
       'connector.version' = '7',
       'connector.hosts' = '%s',
       'connector.index' = 'shafa_pv_uv_dh',
       'connector.document-type' = 'shafa_pv_uv',
       'connector.bulk-flush.max-actions' = '1',
       'format.type' = 'json',
       'update-mode' = 'upsert'
);

insert into