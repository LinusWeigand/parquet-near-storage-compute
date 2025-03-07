WITH
  warehouse_sizes AS (
    SELECT
      warehouseId,
      MAX(
        (scanBytes / NULLIF(scanAssignedFiles, 0)) * scanOriginalFiles
      ) AS estimated_warehouse_size
    FROM
      'snowset-main.parquet/*.parquet'
    GROUP BY
      warehouseId
  ),
  gb_read_per_size AS (
    SELECT
      ws.warehouseId,
      ws.estimated_warehouse_size,
      (
        SUM(s.scanBytes) / NULLIF(ws.estimated_warehouse_size, 0)
      ) AS scanbytes_per_estimated_size
    FROM
      'snowset-main.parquet/*.parquet' s
      JOIN warehouse_sizes ws ON s.warehouseId = ws.warehouseId
    GROUP BY
      ws.warehouseId
  )
SELECT
  SUM(
    scanbytes_per_estimated_size * estimated_warehouse_size
  ) / SUM(estimated_warehouse_size) / 14 * 365 / 12 AS weighted_avg_gb_read_per_warehouse_size SUM(estimated_warehouse_size)
FROM
  warehouse_sizes;
