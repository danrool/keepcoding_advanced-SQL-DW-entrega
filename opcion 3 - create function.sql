CREATE OR REPLACE FUNCTION `keepcoding.fnGetInteger`(value INT64) RETURNS INT64 AS (
IFNULL(value,-999999)
);