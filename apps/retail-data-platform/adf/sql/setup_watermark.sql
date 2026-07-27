CREATE SCHEMA etl;
GO

-- 1. Create the Watermark Table
CREATE TABLE etl.cdc_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_lsn BINARY(10)
);
GO

-- 2. Initialize the Watermark Table with the minimum available LSN for each table
INSERT INTO etl.cdc_watermark (table_name, last_lsn)
VALUES 
    ('orders', sys.fn_cdc_get_min_lsn('dbo_orders')),
    ('customers', sys.fn_cdc_get_min_lsn('dbo_customers')),
    ('inventory', sys.fn_cdc_get_min_lsn('dbo_inventory'));
GO

-- 3. Create the Stored Procedure to update the Watermark after a successful pipeline run
CREATE PROCEDURE etl.sp_update_watermark
    @table_name VARCHAR(100),
    @last_lsn BINARY(10)
AS
BEGIN
    UPDATE etl.cdc_watermark
    SET last_lsn = @last_lsn
    WHERE table_name = @table_name;
END;
GO
