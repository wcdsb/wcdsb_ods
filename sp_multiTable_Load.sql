


Create proc [dbo].[sp_multiTable_Load] @tables nvarchar(max)='', @debug int = 0 as

--2021.02.01:rl - new ported over from pyAspenODS (python solution before link server was given to us)
--2021.02.12:rl - now supports multi tables and renamed proc to sp_multiTable_Load 
--2021.02.09:rl - now after truncating the table it will change the collation of the columns on the tables -- skips OID columns
--2021.02.12:rl  - after new table is created the rename script will now run
--2021.03.13:rl - modified the flow of the script so that data is loaded to a temp table before truncating the table
--2021.05.14:rl - updated debug output 
--2021.06.09:rl - setup init process to load all tables with no data or with data (FULL,TOP10,init)

/**********************************************************************************************************
  Uses the data dictionary to pull active fields and uses the short name instead of the name in the table.  
  Process was "ported" over from a python script that pulled tables we needed.

  to run:               exec sp_multiTable_Load @tables = 'student'
  or multiple tables:   exec sp_multiTable_Load @tables = 'student,person'
**********************************************************************************************************/
                  

if object_id('tempdb..#tables','U') is not null drop table #tables
if object_id('tempdb..#data','U') is not null drop table #data
if object_id('tempdb..#column','U') is not null drop table #column
if object_id('tempdb..#split','U') is not null drop table #split


--declare @tables nvarchar(max) = 'Compass' 
--  ,@debug int = 1

declare @trun nvarchar(max) = ''
        ,@log nvarchar(max) = ''
        ,@load nvarchar(max) = ''
        ,@check nvarchar(max) = ''


select value as [table]
  into #split
from fn_split(@tables,',')

create table #tables ([name]  nvarchar(200)
                    ,parent_object_id nvarchar(20)
                    ,[schema] nvarchar(20))

if @tables in ('init','FULL','TOP10')
begin
  insert into #tables
  select t.[name]
    ,t.parent_object_id
    ,s.name   as [schema]    
  from Aspen.[AspenDB-WCDSB-PD].sys.tables t
  join Aspen.[AspenDB-WCDSB-PD].sys.schemas s
    on t.schema_id = s.schema_id
  where type_desc = 'USER_TABLE'    
    and t.name not in ('ACCESS_LOG','DATA_AUDIT','PERSON_PHOTO','EMAIL_LOG')
    and t.name not like 'Z_%'
  order by t.name
end

if @tables not in ('init','FULL','TOP10')
begin
  insert into #tables
  select t.[name]
    ,t.parent_object_id
    ,s.name   as [schema]    
  from Aspen.[AspenDB-WCDSB-PD].sys.tables t
  join Aspen.[AspenDB-WCDSB-PD].sys.schemas s
    on t.schema_id = s.schema_id
  join #split sp
    on sp.[table] = t.name collate Latin1_General_CI_AS
  where type_desc = 'USER_TABLE'    
    and t.name not in ('ACCESS_LOG','DATA_AUDIT','PERSON_PHOTO','EMAIL_LOG')
  order by t.name
end

if @tables in ('compass','Compass','EnCompass')
begin
  insert into #tables
  select t.[name]
      ,t.parent_object_id
      ,s.name   as [schema]    
    from Aspen.[AspenDB-WCDSB-PD].sys.tables t
    join Aspen.[AspenDB-WCDSB-PD].sys.schemas s
      on t.schema_id = s.schema_id
    join aspen_replication  ar
      on t.[name] = ar.table_name collate SQL_Latin1_General_CP1_CI_AS
    where type_desc = 'USER_TABLE'    
      and t.name not in ('ACCESS_LOG','DATA_AUDIT','PERSON_PHOTO','EMAIL_LOG')
      and t.name not like 'Z_%'
    order by t.name
end

select 
   ROW_NUMBER() OVER(PARTITION BY t.parent_object_id ORDER BY t.name ASC) 
    AS Row#
   ,t.[schema]+'.'+t.name  as obj
   ,'['+t.[schema]+'].'+'['+t.name+']'  as sql_obj
   into #data
from #tables t


declare @i int = 1
  ,@max_i int = 1
  ,@c int = 1
  ,@max_c int =1
  ,@obj nvarchar(max) = ''
  ,@clean_obj nvarchar(max) = ''
  ,@sql_obj nvarchar(max) = ''
  ,@sql nvarchar(max) = ''
  ,@sql_column nvarchar(max) = ''

select @max_i = max(row#) from #data

  
select @max_i
while @i <= @max_i
begin
  select @obj = obj
      ,@sql_obj = sql_obj
  from #data d
  where d.Row# = @i

  set @clean_obj = REPLACE(@obj,'-','_')

  if object_id('tempdb..#column','U') is not null drop table #column
  create table #column ( Row# int
					,coloumn nvarchar(200))

  if @obj <> 'dbo.USER_DEFINED_TABLE_E'
  begin
	  insert into #column

	  --insert into #column
	  SELECT ROW_NUMBER() OVER(PARTITION BY t.parent_object_id ORDER BY c.FDD_SEQUENCE_NUMBER ASC) 
		AS Row#
		,fld_database_name+', '  as coloumn
	  FROM [ASPEN].[AspenDB-WCDSB-PD].[dbo].[DATA_FIELD] f
	  join [ASPEN].[AspenDB-WCDSB-PD].[dbo].[DATA_FIELD_CONFIG] c
		on f.fld_oid = c.fdd_fld_oid
	  join [ASPEN].[AspenDB-WCDSB-PD].dbo.DATA_TABLE dt
		on f.[FLD_TBL_OID] = dt.[TBL_OID] 
	  join #tables t
		on t.name = dt.[TBL_DATABASE_NAME] collate SQL_Latin1_General_CP1_CI_AS
	  where t.[schema]+'.'+t.[name] =@obj
		and c.FDD_USER_NAME_SHORT not like 'Field%'      

		and ( f.FLD_SYSTEM_USE_ONLY_IND = 1
		  or FDD_ENABLED_IND = 1 )
	  order by fld_tbl_oid
	  ,c.FDD_SEQUENCE_NUMBER
	end

	if @obj in ('dbo.USER_DEFINED_TABLE_A','dbo.USER_DEFINED_TABLE_B','dbo.USER_DEFINED_TABLE_C','dbo.USER_DEFINED_TABLE_D','dbo.USER_DEFINED_TABLE_E')
  begin
	  --insert into #column
	  insert into #column
	  SELECT ROW_NUMBER() OVER(PARTITION BY t.parent_object_id ORDER BY c.FDD_SEQUENCE_NUMBER ASC) 
		AS Row#
		,fld_database_name+', '  as coloumn		
	  FROM [ASPEN].[AspenDB-WCDSB-PD].[dbo].[DATA_FIELD] f
	  join [ASPEN].[AspenDB-WCDSB-PD].[dbo].[DATA_FIELD_CONFIG] c
		on f.fld_oid = c.fdd_fld_oid
	  join [ASPEN].[AspenDB-WCDSB-PD].dbo.DATA_TABLE dt
		on f.[FLD_TBL_OID] = dt.[TBL_OID] 
	  join #tables t
		on t.name = dt.[TBL_DATABASE_NAME] collate SQL_Latin1_General_CP1_CI_AS
	  where t.[schema]+'.'+t.[name] =@obj
	
	  order by fld_tbl_oid
	  ,c.FDD_SEQUENCE_NUMBER
	end
  
  -- check if table has timestamp column
  declare @ts_check int

  select @ts_check = count(row#)
  from #column
  where [coloumn] = 'timestamp'

  print 'timestamp_check '+ cast(@ts_check as nvarchar(20))

  declare @add_timestamp_sql nvarchar(max) = ''


IF COL_LENGTH(@clean_obj,'timestamp' ) IS NULL
BEGIN
  -- Column does NOT Exists
      set @add_timestamp_sql = ' alter table Aspen_ODS.'+@obj +' add timestamp datetime '

  exec ( @add_timestamp_sql )
  print @add_timestamp_sql
END

  select @max_c = max(row#) from #column

  while @c<>@max_c
  begin
    select @sql_column += c.coloumn
    from #column c
    where c.Row# = @c
    
    set @c+=1
  end

  -- reset loop
  set @c =1
  set @i += 1
  
  if @debug =1 
    begin
      print @i
      print @clean_obj
    end

    declare @rename_sql nvarchar(max) = ''

  -- Create tables
  set @sql_column = SUBSTRING(@sql_column,1,len(@sql_column)-1)+' '

  if @clean_obj = 'dbo.STUDENT'
  begin
    set @sql = 'select top 0  st.*                   
                 into Aspen_ODS.dbo.STUDENT
                from Aspen_ODS.dbo.vw_wcdsb_student st '
    set @rename_sql = 'exec Aspen_ODS.dbo.sp_RenameColumns @table = '+ @obj
  end
  if @clean_obj <> 'dbo.STUDENT'
  begin    
    set @sql = 'select top 0 '+@sql_column +' , getdate() as timestamp into '+@clean_obj+' from ASPEN.[ASPENDB-WCDSB-PD].'+@obj+''
  end

  -- update student after person table is replicated
  if @clean_obj = 'dbo.PERSON'
  begin
    exec sp_Update_STD_Table
  end  
  
    if @debug =1 
    begin
      select * from #column
      print 'len = ' + cast(len(@sql) as nvarchar(max))
    end
  
  -- create new tables
  if NOT exists ( select * from sys.tables t
                  join sys.schemas s
                    on t.schema_id = s.schema_id
                  where s.[name]+'.'+t.name = @obj )
    begin      
      if @debug =1 
      begin
        print @sql

        print @rename_sql
      end

      exec ( @sql )
      exec ( @rename_sql )
    end

  -- Insert
  if @clean_obj = 'dbo.STUDENT'
  begin   
    set @trun = 'truncate table Aspen_ODS.'+@clean_obj
    set @sql = ' if object_ID(''ASPEN_ODS.tmp.multiLoad'',''U'') is not null drop table Aspen_ODS.tmp.multiLoad '
    set @sql += ' select * into Aspen_ODS.tmp.multiload  from ASPEN_ods.dbo.vw_wcdsb_student st ' 
    set @load = ' insert into Aspen_ODS.'+@clean_obj+' select * from Aspen_ODS.tmp.multiLoad '
  end
  if @clean_obj <> 'dbo.STUDENT'
  begin
    set @trun = ' truncate table Aspen_ODS.'+@clean_obj   
    set @sql = ' if object_ID(''ASPEN_ODS.tmp.multiLoad'',''U'') is not null drop table Aspen_ODS.tmp.multiLoad '    
    if @tables = 'init' set @sql += ' select top 0'+@sql_column +' , getdate() as timestamp into Aspen_ODS.tmp.multiload  from ASPEN.[ASPENDB-WCDSB-PD].'+@obj+' '        
    if @tables = 'TOP10' set @sql += ' select top 10'+@sql_column +' , getdate() as timestamp into Aspen_ODS.tmp.multiload  from ASPEN.[ASPENDB-WCDSB-PD].'+@obj+' '   
    if @tables = 'FULL' set @sql += ' select '+@sql_column +' , getdate() as timestamp into Aspen_ODS.tmp.multiload  from ASPEN.[ASPENDB-WCDSB-PD].'+@obj+' '        
    if @tables not in ('init','FULL','TOP10') set @sql += ' select '+@sql_column +' , getdate() as timestamp into Aspen_ODS.tmp.multiload  from ASPEN.[ASPENDB-WCDSB-PD].'+@obj+' '    
    set @check = ' if object_ID(''Aspen_ODS.'+@clean_obj+ ''',''U'') is NULL select top 0 *   into Aspen_ODS.'+@clean_obj +' from Aspen_ODS.tmp.multiload '
    set @load = ' insert into Aspen_ODS.'+@clean_obj+' select * from Aspen_ODS.tmp.multiLoad '    
  end  
  --
  if exists ( select * from sys.tables t
                  join sys.schemas s
                    on t.schema_id = s.schema_id
                  where s.[name]+'.'+t.name = @obj )
    begin   
      
        if @debug =1 print 'DEBUG INFO: SQL: '+cast(getdate() as nvarchar(20))+' ### '+ @sql      
      exec ( @sql )       -- loads into tmp tables
        if @debug =1 print 'DEBUG INFO: LOG: '+cast(getdate() as nvarchar(20))+' ### '+ @log
      exec ( @log ) 
        if @debug =1 print 'DEBUG INFO: CHECK: '+cast(getdate() as nvarchar(20))+' ### '+ @check      
      exec ( @check )     -- check if table exists and create if need be
        if @debug =1 print 'DEBUG INFO: TRUN: '+cast(getdate() as nvarchar(20))+' ### '+  @trun
      exec ( @trun )      -- truncate table
        if @debug =1 print 'DEBUG INFO: LOAD: '+cast(getdate() as nvarchar(20))+' ### '+ @load      
      exec ( @load )      -- load from tmp into Aspen_ODS
      
    end
      -- reset loop var
  set @sql_column = ''
end


if object_id('tempdb..#tables','U') is not null drop table #tables
if object_id('tempdb..#data','U') is not null drop table #data
if object_id('tempdb..#column','U') is not null drop table #column
