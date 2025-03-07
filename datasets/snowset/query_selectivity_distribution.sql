COPY (
  WITH
    warehouse_size_estimates AS (
      SELECT
        warehouseId,
        MAX(
          (scanBytes / NULLIF(scanFiles, 0)) * scanOriginalFiles
        ) AS estimated_warehouse_size
      FROM
        'snowset-main.parquet/*.parquet'
      GROUP BY
        warehouseId
    ),
    query_selectivity AS (
      SELECT
        q.warehouseId,
        q.scanBytes,
        ws.estimated_warehouse_size,
        CASE
          WHEN ws.estimated_warehouse_size > 0 THEN q.scanBytes / ws.estimated_warehouse_size
          ELSE NULL
        END AS selectivity_ratio
      FROM
        'snowset-main.parquet/*.parquet' q
        JOIN warehouse_size_estimates ws ON q.warehouseId = ws.warehouseId
    ),
    bucketed_selectivity AS (
      SELECT
        warehouseId,
        scanBytes,
        estimated_warehouse_size,
        selectivity_ratio,
        LEAST (CEIL(selectivity_ratio * 100), 100) AS selectivity_bucket
      FROM
        query_selectivity
    ),
    query_counts AS (
      SELECT
        selectivity_bucket,
        COUNT(*) AS query_count
      FROM
        bucketed_selectivity
      GROUP BY
        selectivity_bucket
    ),
    total_queries AS (
      SELECT
        SUM(query_count) AS total
      FROM
        query_counts
    )
  SELECT
    qc.selectivity_bucket,
    qc.query_count,
    (qc.query_count * 100.0) / tq.total AS query_percentage
  FROM
    query_counts qc,
    total_queries tq
  ORDER BY
    qc.selectivity_bucket
) TO 'selectivity_distribution.csv' (FORMAT CSV, HEADER);
