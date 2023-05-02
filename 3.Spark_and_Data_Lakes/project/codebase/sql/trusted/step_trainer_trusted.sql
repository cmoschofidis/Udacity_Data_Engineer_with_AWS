CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_trusted` (
  `serialNumber` string,
  `customerName` string,
  `sensorReadingTime` bigint,
  `distanceFromObject` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-human-bl-analytics/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');