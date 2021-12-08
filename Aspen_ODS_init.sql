/*
	Setup local Apsen replication 
*/


---- CHANGE LOG ---- 
-- 2020-05-13:rl - new
-- 2020-05-14:rl - Populate aspen_replication with WCDSB default 

declare @db_name nvarchar(200)		= 'Aspen_ODS'					        -- Database to be created to house the replication
	,@board nvarchar(20)			= 'WCDSB'					                	-- Board short name
	,@compass_roles bit				= 1								                  -- Create and load compass roles
	,@preload_wcdsb bit				= 1								                  -- Preload with WCDSB tables
	,@link			varchar(200)	= 'Aspen.[AspenDB-WCDSB-PD]'	      -- Link server to Aspen ODS  i.e. Aspen.[AspenDB-WCDSB-PD]
	,@overwrite_db bit				= 0						                      -- 1 - Replace Current Database 
	,@debug		   bit				= 1	                                  -- 1 - turns debug mode on


if object_id('tempdb..#conf','U') is not null drop table #conf

select @db_name  as db
	,@board		 as board
	,@compass_roles   as compass_roles
	,@preload_wcdsb		as preload_wcdsb
	,@link				as link_server
	,@overwrite_db			as overwrite_db
	,@debug				as debug
into #conf


if @debug = 1
begin
	select 'conf'  as source,* from #conf
end


declare @sql_setup varchar(max)  =''

set @sql_setup =' alter database '+  @db_name +' set single_user with rollback immediate'
exec ( @sql_setup )
set @sql_setup = 'drop database '+ @db_name +''
exec ( @sql_setup )


if @overwrite_db <> 1
begin
	set @sql_setup = 'Create Database ' +@db_Name
	exec ( @sql_setup )
end

if @overwrite_db = 1
begin
	set @sql_setup = ' Drop Database '+@db_name	
	if @debug = 1 begin print @sql_setup end
	exec ( @sql_setup )
	set @sql_setup = null -- safety first!

	set @sql_setup = 'Create Database ' +@db_Name	
	if @debug = 1 begin print @sql_setup end
	exec ( @sql_setup )
end

-- Create replication table
set @sql_setup = ' CREATE TABLE '+ @db_name + '.dbo.aspen_replication(
	[id] [int] IDENTITY(1,1) NOT NULL,
	[table_name] [nvarchar](200) NULL,
	[half] [bit] NULL,
	[hourly] [bit] NULL,
	[daily] [bit] NULL
) ON [PRIMARY] '

if @debug = 1 print @sql_setup
exec ( @sql_setup )

if @preload_wcdsb = 1
begin
	set @sql_setup = ' SET IDENTITY_INSERT '+ @db_name +' .dbo.aspen_replication ON '
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (1, ''student'', NULL, 1, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (2, ''PERSON'', NULL, 1, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (3, ''student_contact'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (4, ''school'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (5, ''DISTRICT_SCHOOL_YEAR_CONTEXT'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (6, ''STUDENT_TRANSCRIPT'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (7, ''course_school'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (8, ''student_enrolment'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (9, ''STAFF_POSITION'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (10, ''Staff'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (11, ''student_program_participation'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (12, ''student_alert'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (13, ''person_address'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (14, ''address'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (15, ''schedule_term'', NULL, NULL, NULL)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (16, ''STUDENT_ENROLLMENT'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (17, ''USER_INFO'', NULL, 1, NULL)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (22, ''STUDENT_ATTENDANCE'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (19, ''STUDENT_SCHOOL'', NULL, NULL, NULL)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (20, ''STUDENT_ASSESSMENT'', NULL, NULL, NULL)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (21, ''STUDENT_ASSESSMENT_RESPONSE'', NULL, NULL, NULL)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (22, ''STUDENT_ATTENDANCE'', NULL, NULL, 1)'
	set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (23, ''STUDENT_PERIOD_ATTENDANCE'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (24, ''PERSON_TO_ADDRESS'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (25, ''GRADUATION_STUDENT_PROGRAM'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (26, ''organization'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (27, ''Course'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (28, ''graduation_program'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (29, ''RUBRIC_CRITERION'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (30, ''RUBRIC_DEFINITION'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (30, ''RUBRIC_RATING_SCALE'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (31, ''student_program_detail'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (32, ''student_schedule'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (33, ''Schedule_master'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (34, ''schedule'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (35, ''Schedule_class'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (36, ''schedule_period'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (37, ''schedule_term_date'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (38, ''Contact'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (39, ''Gradebook_Score'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (40, ''GRADEBOOK_COLUMN_DEFINITION'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (41 ''GRADE_TRANS_COLUMN_DEFINITION'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (42, ''GRADE_TERM_DATE'', NULL, NULL, 1)'
  set @sql_setup += 'INSERT '+ @db_name +'.dbo.aspen_replication ([id], [table_name], [half], [hourly], [daily]) VALUES (43, ''GRADUATION_REQUIREMENT'', NULL, NULL, 1)'
   
	set @sql_setup  += ' SET IDENTITY_INSERT '+ @db_name +' .dbo.aspen_replication OFF '	

	if @debug = 1 print @sql_setup
	exec ( @sql_setup )
	
	if @debug = 1 print ( @sql_setup)
	
	set @sql_setup = null
end

-- done with inital table setup
GO


if EXISTS (select *
           from   sys.objects
           where  object_id = OBJECT_ID(N'[dbo].[fn_Split]')
                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))
  drop function [dbo].fn_Split

GO 
-- create split function 
create Function [dbo].[fn_Split]
(@String    nvarchar(max) = NULL
,@Delimiter nvarchar(1)   = ','
) RETURNS @Results TABLE (value nvarchar(max))
 
AS

  -- rl: Use by Aspen replication proc: sp_multiTable_Load
  -- rl: this function takes two parameters; the first is the delimited string, the second is the delimiter
  
    BEGIN
    DECLARE @INDEX INT;
    DECLARE @SLICE nvarchar(4000);
    SELECT @INDEX = 1;
  
    IF @String IS NULL RETURN;
    WHILE @INDEX !=0
        BEGIN                
            SELECT @INDEX = CHARINDEX(@Delimiter,@STRING);            
            IF @INDEX !=0
                SELECT @SLICE = LEFT(@STRING,@INDEX - 1);
            ELSE
                SELECT @SLICE = @STRING;            
            INSERT INTO @Results(value) VALUES(@SLICE);            
            SELECT @STRING = RIGHT(@STRING,LEN(@STRING) - @INDEX);            
            IF LEN(@STRING) = 0 BREAK;
    END;

    RETURN;
END;
Go
-- Create sp_multiTable_Load


select 'conf'  as source,* from #conf


if object_id('tempdb..#conf','U') is not null drop table #conf
