use [Aspen_ODS]
GO

CREATE Function dbo.fn_Split
(@String    nvarchar(max) = NULL
,@Delimiter nvarchar(1)   = ','
) RETURNS @Results TABLE (value nvarchar(max))
with encryption
AS

  --this function takes two parameters; the first is the delimited string, the second is the delimiter
    BEGIN
    DECLARE @INDEX INT;
    DECLARE @SLICE nvarchar(4000);
    -- HAVE TO SET TO 1 SO IT DOESNT EQUAL Z
    --     ERO FIRST TIME IN LOOP
    SELECT @INDEX = 1;
  
    IF @String IS NULL RETURN;
    WHILE @INDEX !=0


        BEGIN    
            -- GET THE INDEX OF THE FIRST OCCURENCE OF THE SPLIT CHARACTER
            SELECT @INDEX = CHARINDEX(@Delimiter,@STRING);
            -- NOW PUSH EVERYTHING TO THE LEFT OF IT INTO THE SLICE VARIABLE
            IF @INDEX !=0
                SELECT @SLICE = LEFT(@STRING,@INDEX - 1);
            ELSE
                SELECT @SLICE = @STRING;
            -- PUT THE ITEM INTO THE RESULTS SET
            INSERT INTO @Results(value) VALUES(@SLICE);
            -- CHOP THE ITEM REMOVED OFF THE MAIN STRING
            SELECT @STRING = RIGHT(@STRING,LEN(@STRING) - @INDEX);
            -- BREAK OUT IF WE ARE DONE
            IF LEN(@STRING) = 0 BREAK;
    END;

    RETURN;
END;
GO


use [Aspen_ODS]
GO





create proc [dbo].[sp_Change_Collation] @table nvarchar(200) = '' as


-- This proc is used to change the collate on non key columns

-- 2021.02.09:RL - new


if object_id('tempdb..#data','U') is not null drop table #data


declare @debug int = 1

declare @i int = 1
		,@i_max int = 1
		,@sql nvarchar(max) = ''

select ROW_NUMBER() OVER(order by t.name ASC)     AS Row#
	,t.name as table_name
	,t.object_id
	,s.name   as [schema]
	,s.name+'.'+t.name as obj
	,c.name as [column_name]
	,c.collation_name
	,c.system_type_id
	,c.user_type_id
	,c.max_length
	,'Alter TABLE ASPEN_ODS.'+s.name+'.'+t.name+' ALTER COLUMN ['+ c.name +'] nvarchar('+case when c.max_length = -1 then 'MAX' else cast(c.max_length as  nvarchar(20)) end +') COLLATE SQL_Latin1_General_CP1_CI_AS' as SQL_COLLATE
  into #data
from aspen_ods.sys.tables t
join aspen_ods.sys.schemas s
  on t.schema_id = s.schema_id
join aspen_ods.sys.columns c
  on t.object_id = c.object_id
where s.name+'.'+t.name = @table
	and c.name not like '%OID%'	
	and c.collation_name <> 'SQL_Latin1_General_CP1_CI_AS'

select @i_max = max(row#) from #data

if @debug = 1 
begin
	select * from #data	
end

while @i <= @i_max
begin
	select @sql = d.SQL_COLLATE
	from #data d
	where d.Row# = @i

	if @debug = 1 
	begin		
		print ( @sql )
		print @i
	end
	
	exec ( @sql )
	set @i += 1
end

if object_id('tempdb..#data','U') is not null drop table #data
GO



use [Aspen_ODS]
GO



create proc [dbo].[sp_RenameColumns] @table nvarchar(20) as


if object_id('tempdb..#tables','U') is not null drop table #tables
if object_id('tempdb..#data','U') is not null drop table #data
if object_id('tempdb..#update','U') is not null drop table #update


-- sys tables
select t.*
  ,s.name   as [schema]
  ,s.name+'.'+t.name as obj
  into #tables
from Aspen.[AspenDB-WCDSB-PD].sys.tables t
join Aspen.[AspenDB-WCDSB-PD].sys.schemas s
  on t.schema_id = s.schema_id
where type_desc = 'USER_TABLE'
  and s.name+'.'+t.name  = @table 

order by t.name


-- apsen data 
SELECT [FLD_OID]
      ,[FLD_TBL_OID]
      ,c.FDD_USER_NAME_SHORT
      ,f.fld_database_name
      ,t.obj
      ,dt.[TBL_DATABASE_NAME]
      ,t.[schema]+'.'+t.[name]      as [DB]
      ,case when c.FDD_USER_NAME_SHORT  like '%?' then 'IS_'+ substring(c.FDD_USER_NAME_SHORT,1,len(c.FDD_USER_NAME_SHORT)-1) 
            when c.FDD_USER_NAME_SHORT like '% %' then replace(FDD_USER_NAME_SHORT,' ','_')
            else c.FDD_USER_NAME_SHORT 
        end  as CleanName
      
  into #data
FROM [ASPEN].[AspenDB-WCDSB-PD].[dbo].[DATA_FIELD] f
join [ASPEN].[AspenDB-WCDSB-PD].[dbo].[DATA_FIELD_CONFIG] c
  on f.fld_oid = c.fdd_fld_oid

join [ASPEN].[AspenDB-WCDSB-PD].dbo.DATA_TABLE dt
  on f.[FLD_TBL_OID] = dt.[TBL_OID] 
join #tables t
  on t.name = dt.[TBL_DATABASE_NAME] collate SQL_Latin1_General_CP1_CI_AS
where c.FDD_USER_NAME_SHORT not like 'Field%'      
  and c.FDD_USER_NAME_SHORT not like '%OID%'
  and c.FDD_USER_NAME_SHORT <> 'ID'
  and ( f.FLD_SYSTEM_USE_ONLY_IND = 1 
    or FDD_ENABLED_IND = 1 )
order by fld_tbl_oid




select 
  ROW_NUMBER() OVER(order by tbl_database_name ASC)     AS Row#
  ,'sp_rename '''+d.obj collate SQL_Latin1_General_CP1_CI_AS+'.'+fld_database_name+''','''+d.CleanName+''', ''COLUMN'''  as [SQL_CMD]
  into #update
from #data d

declare @i int = 1
  ,@max_i int = 1
  ,@sql nvarchar(max) = ''

select @max_i = max(row#) from #update

select * from #update

while @i <= @max_i
begin

  select @sql = u.SQL_CMD
  from #update u
  where u.Row# = @i

  exec ( @sql )
  print @sql

  set @i +=1
  set @sql = ''
end



if object_id('tempdb..#tables','U') is not null drop table #tables
if object_id('tempdb..#data','U') is not null drop table #data
if object_id('tempdb..#update','U') is not null drop table #update





go



create proc [dbo].[sp_Update_STD_Table] as

-- Used to update Student table with information from persons and enrolment

update st
  set st.[Legal_Surname] =     p.Legal_Last_Name 
      ,st.Legal_Firstname  = p.Legal_First_Name 
      ,st.psn_email_01 = p.Email1 
      ,st.psn_gender_code = p.Gender 
      ,st.psn_dob = p.DOB
      ,st.[Catholic?] = p.IS_Catholic   
      ,st.[Status in Canada] = p.Status_in_Canada
      ,st.[CurrentSchool] = sc.school_code
      ,st.[Surname] = p.Last_Name 
      ,st.[Firstname]  = p.First_Name       
from Aspen_ODS.dbo.STUDENT st
join Aspen_ODS.[dbo].[Person] p 
  on p.psn_oid = st.std_psn_oid 
join Aspen_ODS.[dbo].[SCHOOL] sc 
  on st.STD_SKL_OID = sc.skl_oid


--Isidore
update std
  set std.Isidore = 1
from STUDENT_SCHOOL ssk
join school skl
  on ssk.SSK_SKL_OID = skl.SKL_OID
  and ssk.Arrival_Status = 'Arrived'
join STUDENT std
  on ssk.SSK_STD_OID = std.STD_OID
where skl.name like '%Isidore%'
  and cast(getdate() as date) between ssk.[start] and ssk.[end]



update std
  set std.Arrival_Status =  enr.Arrival_Status
from student std
join STUDENT_ENROLLMENT enr
  on std.STD_OID = enr.ENR_STD_OID
  and std.STD_SKL_OID = enr.ENR_SKL_OID
where std.EnrStatus = 'Active'
  --and enr.Arrival_Status = 'Arrived'
  and Date >= '2020-09-08'

--exec sp_multiTable_Load @tables = 'STUDENT_SCHOOL,ORGANIZATION'
 




if object_id('tempdb..#form','U') is not null drop table #form


GO

use [Aspen_ODS]
GO





Create proc [dbo].[sp_Aspen_Replication_Interval] @interval nvarchar(200) = 'Hourly' , @debug int = 0 as

if object_id('tempdb..#replication','U') is not null drop table #replication 


--declare @interval nvarchar(200) = 'daily'

declare @bug int = 0
set @bug = @debug

-- init table
select top 0 ROW_NUMBER() OVER(order by table_name ASC)     AS Row#    
 ,table_name  
into #replication
from wcdsb.dbo.aspen_replication rp


if @interval = 'hourly'
begin
  insert into #replication
  select  ROW_NUMBER() OVER(order by table_name ASC)     AS Row#
    ,table_name  
  from wcdsb.dbo.aspen_replication rp
  where rp.hourly = 1
end

if @interval = 'daily'
begin
  insert into #replication
  select  ROW_NUMBER() OVER(order by table_name ASC)     AS Row#
    ,table_name  
  from wcdsb.dbo.aspen_replication rp
  where rp.daily = 1
end


declare @sql_table nvarchar(max) = ''
  ,@i int = 1 
  ,@i_max int = 1


select @i_max = max(r.Row#) from #replication r

while @i <= @i_max
begin
  select @sql_table += +rp.table_name+','
  from #replication rp
  where rp.Row# = @i 

  print @sql_table
  
  set @i+=1
end

set @sql_table = upper(substring(@sql_table,1,len(@sql_table)-1))

print 'running '+ @sql_table
exec Aspen_ODS.dbo.sp_multiTable_Load  @tables = @sql_table ,@debug = @bug

if object_id('tempdb..#replication','U') is not null drop table #replication 
GO

create schema tmp

go

use Aspen_ODS
go

create schema aspen 
go

USE [Aspen_ODS]
GO


create view [dbo].[vw_wcdsb_student] as
select [STD_OID]
      ,[STD_ORG_OID_1]
      ,[STD_ORG_OID_2]
      ,[STD_ORG_OID_3]
      ,[STD_ORG_OID_4]
      ,[STD_ORG_OID_5]
      ,[STD_PSN_OID]
      ,[STD_SKL_OID]
      ,[STD_SKL_OID_NEXT]
      ,[STD_SKL_OID_SUMMER]
      ,[STD_SCA_OID]
      ,[STD_SCA_OID_NEXT]
      ,[STD_CTJ_OID_1]
      ,[STD_CTJ_OID_2]
      ,[STD_CTJ_OID_3]
      ,[STD_SXA_OID_CURRENT]
      ,[STD_GUID]
      ,[STD_GUID_2]
      ,[STD_GUID_3]
      ,[STD_NAME_VIEW]
      ,[STD_ADRS_VIEW]
      ,[STD_ID_LOCAL]
      ,[STD_ID_STATE]
      ,[STD_YOG]
      ,[STD_GRADE_LEVEL]
      ,[STD_HOMEROOM]
      ,[STD_HOMEROOM_NEXT]
      ,[STD_HR_TEACHER_VIEW]
      ,[STD_HR_NEXT_TEACHER_VIEW]
      ,[STD_LOCKER]
      ,[STD_ENROLLMENT_STATUS]
      ,[STD_ENROLLMENT_TYPE_CODE]
      ,[STD_PROGRAM_OF_STUDY_CODE]
      ,[STD_HOME_LANGUAGE_CODE]
      ,[STD_CALENDAR_CODE]
      ,[STD_TRANSFER_PENDING_IND]
      ,[STD_RANK_INCLUDE_IND]
      ,[STD_HONOR_ROLL_IND]
      ,[STD_SPED_STATUS]
      ,[STD_SPED_TYPE]
      ,[STD_SPED_REFERRAL]
      ,[STD_SPED_INITIAL_ELIGIBILITY]
      ,[STD_SPED_LAST_ELIGIBILITY]
      ,[STD_SPED_LAST_REVIEW]
      ,[STD_SPED_NEXT_REVIEW]
      ,[STD_SPED_LAST_EVALUATION]
      ,[STD_SPED_NEXT_EVALUATION]
      ,[STD_SPED_EXIT_DATE]
      ,[STD_ACADEMIC_TRACK_TYPE_CODE]
      ,[STD_ID_MED]
      ,[STD_ALERT_VIEW]
      ,[STD_QUICK_STATUS]
      ,[STD_COURIER_IND]
      ,[STD_AT_RISK_VIEW]
      ,[STD_MIGRATION_DATE]
      ,[STD_GRADUATION_HISTORY_NOTES]
      ,[STD_FIELDA_001]
      ,[STD_FIELDA_002]
      ,[STD_FIELDA_003]
      ,[STD_FIELDA_004]
      ,[STD_FIELDA_005]
      ,[STD_FIELDA_006]
      ,[STD_FIELDA_007]
      ,[STD_FIELDA_008]
      ,[STD_FIELDA_009]
      ,[STD_FIELDA_010]
      ,[STD_FIELDA_011]
      ,[STD_FIELDA_012]
      ,[STD_FIELDA_013]
      ,[STD_FIELDA_014]
      ,[STD_FIELDA_015]
      ,[STD_FIELDA_016]
      ,[STD_FIELDA_017]
      ,[STD_FIELDA_018]
      ,[STD_FIELDA_019]
      ,[STD_FIELDA_020]
      ,[STD_FIELDA_021]
      ,[STD_FIELDA_022]
      ,[STD_FIELDA_023]
      ,[STD_FIELDA_024]
      ,[STD_FIELDA_025]
      ,[STD_FIELDA_026]
      ,[STD_FIELDA_027]
      ,[STD_FIELDA_028]
      ,[STD_FIELDA_029]
      ,[STD_FIELDA_030]
      ,[STD_FIELDA_031]
      ,[STD_FIELDA_032]
      ,[STD_FIELDA_033]
      ,[STD_FIELDA_034]
      ,[STD_FIELDA_035]
      ,[STD_FIELDA_036]
      ,[STD_FIELDA_037]
      ,[STD_FIELDA_038]
      ,[STD_FIELDA_039]
      ,[STD_FIELDA_040]
      ,[STD_FIELDA_041]
      ,[STD_FIELDA_042]
      ,[STD_FIELDA_043]
      ,[STD_FIELDA_044]
      ,[STD_FIELDA_045]
      ,[STD_FIELDA_046]
      ,[STD_FIELDA_047]
      ,[STD_FIELDA_048]
      ,[STD_FIELDA_049]
      ,[STD_FIELDA_050]
      ,[STD_FIELDA_051]
      ,[STD_FIELDA_052]
      ,[STD_FIELDA_053]
      ,[STD_FIELDA_054]
      ,[STD_FIELDA_055]
      ,[STD_FIELDA_056]
      ,[STD_FIELDA_057]
      ,[STD_FIELDA_058]
      ,[STD_FIELDA_059]
      ,[STD_FIELDA_060]
      ,[STD_FIELDA_061]
      ,[STD_FIELDA_062]
      ,[STD_FIELDA_063]
      ,[STD_FIELDA_064]
      ,[STD_FIELDA_065]
      ,[STD_FIELDA_066]
      ,[STD_FIELDA_067]
      ,[STD_FIELDA_068]
      ,[STD_FIELDA_089]
      ,[STD_FIELDA_090]
      ,[STD_FIELDA_091]
      ,[STD_FIELDA_092]
      ,[STD_FIELDA_093]
      ,[STD_FIELDA_094]
      ,[STD_FIELDA_095]
      ,[STD_FIELDA_096]
      ,[STD_FIELDA_097]
      ,[STD_FIELDA_098]
      ,[STD_FIELDA_099]
      ,[STD_FIELDA_100]
      ,[STD_FIELDB_001]
      ,[STD_FIELDB_002]
      ,[STD_FIELDB_003]
      ,[STD_FIELDB_004]
      ,[STD_FIELDB_005]
      ,[STD_FIELDB_006]
      ,[STD_FIELDB_007]
      ,[STD_FIELDB_008]
      ,[STD_FIELDB_009]
      ,[STD_FIELDB_010]
      ,[STD_FIELDB_011]
      ,[STD_FIELDB_012]
      ,[STD_FIELDB_014]
      ,[STD_FIELDB_015]
      ,[STD_FIELDB_016]
      ,[STD_FIELDB_017]
      ,[STD_FIELDB_018]
      ,[STD_FIELDB_022]
      ,[STD_FIELDB_024]
      ,[STD_FIELDB_025]
      ,[STD_FIELDB_026]
      ,[STD_FIELDB_027]
      ,[STD_FIELDB_028]
      ,[STD_FIELDB_029]
      ,[STD_FIELDC_001]
      ,[STD_FIELDC_002]
      ,[STD_FIELDC_003]
      ,[STD_FIELDC_004]
      ,[STD_FIELDC_005]
      ,[STD_FIELDC_006]
      ,[STD_FIELDC_007]
      ,[STD_FIELDC_008]
      ,[STD_FIELDC_009]
      ,[STD_FIELDC_010]
      ,[STD_FIELDE_001]
      ,[STD_FIELDA_128]
      ,[STD_FIELDA_131]
      ,[STD_FIELDA_132]
      ,[STD_FIELDA_134]
      ,[STD_FIELDA_135]
      ,null as [Legal_Surname] 
      ,null as [Legal_Firstname] 
      ,null as psn_email_01 
      ,null as nullpsn_gender_code 
      ,null as psn_dob
      ,null as [Catholic?]  
      ,null as [Status in Canada]      
      ,null as [CurrentSchool]
      ,null as Arrival_Status
      ,null as [Surname]
      ,null as [Firstname] 
      ,null as [Isidore]
      ,getdate()  as [timestamp]
      ,null as [Delivery Model]
      ,null as Extended
from [ASPEN].[AspenDB-WCDSB-PD].[dbo].student st

GO



