use Datawarehouse;
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	BEGIN try
		set @batch_start_time=GETDATE();
		
		print 'loading bronze layer';

		print'loading CRM tables----------------'

		set @start_time=GETDATE();
		truncate table bronze.crm_cust_info;
		Bulk insert bronze.crm_cust_info
		from 'C:\Users\dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =GETDATE();
		PRINT 'load duration' + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + 'seconds';


		set @start_time=GETDATE();
		print'__LOADING TIME__'
		truncate table bronze.crm_prd_info;
		Bulk insert bronze.crm_prd_info
		from 'C:\Users\dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =GETDATE();
		PRINT 'load duration' + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +'seconds';




		set @start_time=GETDATE();
		truncate table bronze.crm_sales_details;
		Bulk insert bronze.crm_sales_details
		from 'C:\Users\dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =GETDATE();
		PRINT 'load duration' + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +'seconds';


		print'loading erp tables----------------'
	
		set @start_time=GETDATE();
		print'__LOADING TIME__'
		truncate table bronze.erp_loc_a101;
		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =GETDATE();
		PRINT 'load duration' + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +'seconds';


		set @start_time=GETDATE();
		print'__LOADING TIME__'
		truncate table bronze.erp_cust_az12;
		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =GETDATE();
		PRINT 'load duration' + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +'seconds';

		set @start_time=GETDATE();
		print'__LOADING TIME__'
		truncate table bronze.erp_px_cat_g1v2;
		Bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\dell\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		set @end_time =GETDATE();
		PRINT 'load duration ' + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) +'seconds';

		set @batch_end_time=GETDATE();
		print 'batch load durationn ' + cast(DATEDIFF(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds';
		END try
	    BEGIN catch
		print'=======ERROR OCCCURED DURING LOADING======'
		print 'error message'+ERROR_MESSAGE();
		print 'error message'+cast (ERROR_NUMBER() as nvarchar);
		print 'error message'+cast (ERROR_STATE() as nvarchar);
		END catch
end
