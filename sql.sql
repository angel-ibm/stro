SELECT
    json_extract_scalar(_message, '$.image_format') AS format,
    json_extract_scalar(_message, '$.file') AS file
FROM
  "kafka"."default"."fits-images"
LIMIT
  100;

create table iceberg_data.angel."fits-images" as
(
    SELECT
        json_extract_scalar(_message, '$.image_format') AS "image_format",
        json_extract_scalar(_message, '$.file') AS "file",
        json_extract_scalar(_message, '$.image_data') AS "image_data"
    FROM
        "kafka"."default"."fits-images"
)