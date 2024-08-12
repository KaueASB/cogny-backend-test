DROP VIEW if exists ${schema:raw}.vw_population_summary CASCADE;

CREATE OR REPLACE VIEW ${schema:raw}.vw_population_summary AS
SELECT SUM((records->>'Population')::integer) AS total_population
    FROM (
        SELECT jsonb_array_elements(doc_record) AS records
        FROM ${schema:raw}.api_data
    )
WHERE records->>'Year' IN ('2020', '2019', '2018');
