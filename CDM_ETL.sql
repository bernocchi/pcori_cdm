--create sequences used for matching valid non duplicated patient IDs
create sequence manuel.tmp_pat_id_clarity2CDM 
 minvalue 1 
 maxvalue 6000000 
 start with 1 
 increment by 1 
cache 100;
create sequence manuel.tmp_randTracker 
 minvalue 1 
 maxvalue 6000000 
 start with 1 
 increment by 1 
 cache 100;

--create temp table of random IDs referenced by sequence number
create table  manuel.tmp_rand_ids as (
  select manuel.tmp_randTracker.nextval seq, id from(
    select distinct id from(
      select trunc(dbms_random.value(999999,9999999999)) id from dual CONNECT BY level <= 600000)));

--create temp table of date shifts, epic IDs referenced by sequence number
--note that this temp table is required because of oracle's restrictions on the 
--use of sequences. 
create table manuel.tmp_patid_clarity2cdm as 
  select manuel.tmp_pat_id_clarity2CDM.nextval seq,
    pat_id, trunc(dbms_random.value(-1,-365)) as date_shift 
  from clarity.patient;

--join the two tables on the sequence number. This ensures that each patient has a nice randomly
--generated, non clashing id. :) 
create table manuel.Clarity_to_cdm_ptmap as (
select patid_ide, patid, date_shift from 
    manuel.tmp_patid_clarity2cdm t
   join 
    manuel.tmp_rand_ids r on t.seq = r.seq
);

--clean up -- something Heron forgets. 
drop table manuel.tmp_patid_clarity2cdm;
drop table manuel.tmp_rand_ids;
drop sequence manuel.tmp_randTracker;
drop sequence manuel.tmp_pat_id_clarity2CDM;



-- DEMOGRAPHICS
--INSERT INTO DEMOGRAPHICS
SELECT F.patID,
       TRUNC(BIRTH_DATE) + DATE_SHIFT AS BIRTH_DATE,
       TO_CHAR(BIRTH_DATE, 'HH24:MM') AS BIRTH_TIME,
       CASE E.NAME
           WHEN 'Female' THEN 'F'
           WHEN 'Male'   THEN 'M'
           ELSE 'UN'
       END SEX,
       CASE B.NAME
          WHEN 'Hispanic or Latino' THEN 'Y'
          WHEN 'I choose not to provide this information' THEN 'R'
          WHEN 'Unknown/Other' THEN 'UN'
          WHEN 'Unknown' THEN 'UN'
          ELSE 'N'
       END HISPANIC,
       CASE D.NAME
           WHEN 'American Indian or Alaska Native' THEN '01'
           WHEN 'Asian'                            THEN '02'
           WHEN 'Black or African-American'        THEN '03'
           WHEN 'Native Hawaiian and Other Pacific Islander' THEN '04'
           WHEN 'White or Caucasian'               THEN '05'
           WHEN 'More Than One Race'               THEN '06'
           WHEN 'Patient Refused'                  THEN '07'
           WHEN 'I choose not to provide this information' THEN '07'
           WHEN 'Unknown'                          THEN 'UN'
           WHEN 'Unknown/Other'                    THEN 'UN'
           WHEN 'Other'                            THEN 'OT'
           ELSE 'NI'
       END RACE,
       'N' AS BIOBANK_FLAG,
       NULL AS RAW_SEX,
       NULL AS RAW_HISPANIC,
       NULL AS RAW_RACE
FROM   CLARITY.PATIENT A
INNER JOIN
       CLARITY.ZC_ETHNIC_GROUP B
ON     A.ETHNIC_GROUP_C = B.ETHNIC_GROUP_C
INNER JOIN
       CLARITY.PATIENT_RACE C
ON     A.PAT_ID = C.PAT_ID
INNER JOIN
      CLARITY.ZC_PATIENT_RACE D
ON    C.PATIENT_RACE_C = D.PATIENT_RACE_C      
INNER JOIN
      CLARITY.ZC_SEX E
ON    A.SEX_C = E.RCPT_MEM_SEX_C
INNER JOIN
      manuel.Clarity_to_cdm_ptmap F
      ON A.PAT_ID = F.PATID_ide
;
create sequence manuel.tmp_pat_enc_id_clarity2CDM 
 minvalue 9999999 
 maxvalue  999999999
 start with 1 
 increment by 1 
cache 100;


CREATE TABLE MANUEL.ENC_ID_CLARITY2CDM
SELECT  manuel.tmp_pat_enc_id_clarity2CDM.NEXTVAL AS ENCOUNTERID, 
        PAT_ENC_CSN_ID AS ENCOUNTERID_IDE
FROM    CLARITY.PAT_ENC;



CREATE SEQUENCE MANUEL.TMP_PROV_ID_CLARITY2CDM
MIN VALUE 10000
MAX VALUE 999999999
START WITH 10000
INCREMENT BY 1
cache 100;


CREATE  TABLE MANUEL.Clarity_to_cdm_provtmap 
SELECT  MANUEL.TMP_PROV_ID_CLARITY2CDM.NEXTVAL PROVIDERID,
        VISIT_PROVIDER_ID AS PROVIDERID_IDE
FROM (
SELECT DISTINCT VISIT_PROVIDER_ID
FROM CLARITY.PAT_ENC
)
;

SELECT  ENCOUNTERID,
        ptmap.ID        AS PATID,
        TRUNC(CONTACT_DATE) + DATE_SHIFT AS ADMIT_DATE, 
        CASE CHECKIN_TIME
             WHEN IS NOT NULL THEN TO_CHAR(CHECKIN_TIME, 'HH24:MM')
             ELSE '00:00'
        END ADMIT_TIME,
        TRUNC(CONTACT_DATE)    AS DISCHARGE_DATE,
        TO_CHAR(CONTACT_DATE, 'HH24:MM') AS DISCHARGE_TIME,
        PROVIDERID,
        NULL            AS FACILITYID,
        'NI'            AS DISCHARGE_DISPOSITION,
        (SELECT CDM_CODE 
         FROM   MAPPING_FILES.CDM_ENC_TYPE_MAPPING@CLARITY_AT_EPIC.MI 
         WHERE NVL(ENC_CODE, 9999999) = NVL(PAT_ENC.ENC_TYPE_C, 9999999) ) 
                        AS ENC_TYPE,
       'UTM'            AS FACILITYID,
        'NI'            AS DISCHARGE_DISPOSITION,
        'NI'            DISCHARGE_STATUS,
        NULL            DRG,
        'NI'            DRG_TYPE,
        'NI'            ADMITTING_SOURCE,
        NULL            RAW_SITEID,
        NULL            RAW_ENC_TYPE,
        NULL            RAW_DISCHARGE_DISPOSITION,
        NULL            RAW_DISCHARGE_STATUS,
        NULL            RAW_DRG_TYPE,
        NULL            RAW_ADMITTING_SOURCE
FROM    CLARITY.PAT_ENC ENC
INNER JOIN
        manuel.Clarity_to_cdm_ptmap ptmap
ON      ENC.PAT_ID = PTMAP.PATID_ide
INNER JOIN
        MANUEL.ENC_ID_CLARITY2CDM ENCMAP
ON      ENCMAP.ENCOUNTERID_IDE = ENC.PAT_ENC_CSN_ID
INNER JOIN
        MANUEL.Clarity_to_cdm_provtmap PROVMAP
ON      PROVMAP.PROVIDERID_IDE = ENC.VISIT_PROVIDER_ID
;

INSERT INTO ENROLLMENT
SELECT  PATID                  
        MIN(ADMIT_DATE)         AS ENR_START_DATE, 
        MAX(DISCHARGE_DATE)     AS ENR_END_DATE,
        'Y'                     AS CHART,
        'A'                     AS ENR_BASIS     
FROM    PCORI_CDM.ENCOUNTER
GROUP BY PAT_ID
HAVING  MAX(DISCHARGE_DATE) - MIN(ADMIT_DATE) > 30
;


CREATE SEQUENCE PCORI_CDM.DIAGNOSISID_SEQ
MIN VALUE 39712445
MAX VALUE 9999999999
START WITH 39712445
INCREMENT BY 1
cache 100;

--INSERT INTO DIAGNOSIS
SELECT  PCORI_CDM.DIAGNOSISID_SEQ.NEXTVAL  AS DIAGNOSISID,
        A.PATID, 
        A.ENCOUNTERID,
        A.ENC_TYPE,
        A.ADMIT_DATE,
        A.PROVIDERID,
        C.ICD9_CODE     AS DX,
        '09'            AS DX_TYPE,
        'OT'            AS DX_SOURCE,
        'X'             AS PDX,
        NULL            AS RAW_DX,
        NULL            AS RAW_DX_TYPE,
        NULL            AS RAW_DX_SOURCE,
        NULL            AS RAW_PDX
FROM    PCORI_CDM.ENCOUNTER A
INNER JOIN
        MANUEL.ENC_ID_CLARITY2CDM B
ON      A.ENCOUNTERID = B.ENCOUNTERID 
INNER JOIN
        CLARITY.PAT_ENC_DX C
ON      B.ENCOUNTERID_IDE = C.PAT_ENC_CSN_ID;
;


CREATE SEQUENCE PCORI_CDM.VITALID_SEQ
MIN VALUE 100000
MAX VALUE 9999999999
START WITH 100000
INCREMENT BY 1
cache 100;


SELECT  --PCORI_CDM.VITALID_SEQ.NEXTVAL AS VITALID,
        E.PATID,
        E.ENCOUNTERID,
        E.ADMIT_DATE     AS MEASURE_DATE,
        E.ADMIT_TIME     AS MEASURE_TIME,
        'HC'                      AS VITAL_SOURCE,
         -- Convert Height XX' Y.Y" TO NUMERIC INCHES
        (TO_NUMBER(SUBSTR(HEIGHT, 1, (INSTR(HEIGHT, '''') - 1))) * 12) +  TO_NUMBER(REGEXP_REPLACE(SUBSTR( HEIGHT, INSTR(HEIGHT, ' ') + 1),'"','')) AS HT,
        -- Convert Weight Ounces to Pounds
        A.WEIGHT/16                  AS WT,
        A.BP_DIASTOLIC            AS DIASTOLIC,
        A.BP_SYSTOLIC             AS SYSTOLIC,
        A.BMI                     AS ORIGINAL_BMI,
        'NI'                      AS BP_POSITION,
        CASE
              WHEN B.CIGARETTES_YN = 'Y' OR B.PIPES_YN = 'Y' OR B.CIGARS_YN = 'Y' 
                  THEN CASE C.NAME
                        WHEN 'Quit'     THEN '03'
                        WHEN 'Passive'  THEN 'OT'
                        WHEN 'Never'    THEN '04'
                        WHEN 'Not Asked' THEN 'NI'
                        WHEN 'Yes'      THEN '05'
                       END
              ELSE '04'
        END SMOKING,
        CASE C.NAME
                WHEN 'Quit'     THEN '03'
                WHEN 'Passive'  THEN '04'
                WHEN 'Never'    THEN '02'
                WHEN 'Not Asked' THEN 'NI'
                WHEN 'Yes'      THEN '01'
                ELSE 'NI'
        END TOBACCO,
        CASE
               WHEN B.CIGARETTES_YN = 'Y' OR B.PIPES_YN = 'Y' OR B.CIGARS_YN = 'Y' THEN '01'
               WHEN B.SNUFF_YN = 'Y' OR B.CHEW_YN = 'Y' THEN '02'
               ELSE '04'
        END TOBACCO_TYPE,        
        NULL                    AS RAW_DIASTOLIC,
        NULL                    AS RAW_SYSTOLIC,
        NULL                    AS RAW_BP_POSITION,
        NULL                    AS RAW_SMOKING,
        NULL                    AS RAW_TOBACCO
FROM    CLARITY.PAT_ENC A
LEFT OUTER JOIN
        CLARITY.SOCIAL_HX B
ON      A.PAT_ENC_CSN_ID = B.PAT_ENC_CSN_ID
LEFT OUTER JOIN
        CLARITY.ZC_TOBACCO_USER C
ON      B.TOBACCO_USER_C = C.TOBACCO_USER_C
INNER JOIN
        MANUEL.ENC_ID_CLARITY2CDM D
ON      A.PAT_ENC_CSN_ID = D.ENCOUNTERID_IDE
INNER JOIN
        PCORI_CDM.ENCOUNTER E
ON      D.ENCOUNTERID = E.ENCOUNTERID



SELECT  B.PATID,
        DEATH_DATE + B.DATE_SHIFT AS DEATH_DATE,
        'N'             AS DEATH_DATE_IMPUTE,
        'L'             AS DEATH_SOURCE,
        'E'             AS DEATH_MATCH_CONFIDENCE
FROM    CLARITY.PATIENT A
INNER JOIN
        manuel.Clarity_to_cdm_ptmap B
ON      A.PAT_ID = B.PATIENTID_IDE
WHERE   DEATH_DATE IS NOT NULL;





--Facility Billing 

-- Skip invoices with multiple patients or encounters for the same charge slip number--
--Trash erroneous looking charges.
create table manuel.tmp_bill_mrn_dup as
   select charge_slip_number
   from (
      select distinct charge_slip_number, int_pat_id
      from clarity.CLARITY_TDL_TRAN
      where charge_slip_number is not null
   )
   group by charge_slip_number
   having count(*) > 1;
--is this even an issue? Are there reasons behind these multiple encounters per charge? 
     -- e.g. dialysis or chemotherapy
/*create table manuel.tmp_bill_enc_dup as
   select charge_slip_number
   from (
      select distinct charge_slip_number, pat_enc_csn_id
      from clarity.CLARITY_TDL_TRAN
      where charge_slip_number is not null
   )
   group by charge_slip_number
   having count(*) >1;*/

--create the sequence 
create sequence proceduresID_seq as
 min 10000000
 max 999999999
 increment by 1
 start with 10000000
 cache 100;
 
 --insert facility billing
--insert into pcori_cdm.procedures (
   select --this additional select is needed to weed out errors and multiple row numbers
         proceduresID,
         patid,
         encounterid,
         enc_type,
         admit_date,
         providerid,
         admit_date, --They want this date to match the admit date *shrugs*
         PX,
         PX_TYPE,
         'BI' PX_Source,
         NULL raw_px,
         NULL raw_px_type
   from (
       select
         proceduresid_seq.nextval proceduresID,
         e.patid,
         e.encounterid,
         e.enc_type,
         e.admit_date,
         e.providerid,
         e.admit_date, --They want this date to match the admit date *shrugs*
         CPT_CODE PX
            -- Link too large, see above.
            --the link provides translation for the actual levels.. which become useless in CDM 3.1
            -- thanks for making us do this work PCORI ;)
            --CPT code
         , case when regexp_like(trx.cpt_code,'^[0-9]{5}$') then 'C1' 
                when regexp_like(trx.cpt_code,'^\+?[0-9]{4}[A-Z]$') then 'C2' --Or C3... 
                      --there is no way to tell without comparing it to the "current" list
                      --(note that stents could have  + to indicate additional ones.. -.-; 
              when regexp_like(trx.cpt_code,'^[A-V][0-9]{4}$') then 'HC' 
                   -- hcpcs- No level info in the link above
              else 'ERROR' end PX_TYPE
         , row_number() over
              (partition by trx.PAT_ENC_CSN_ID, CPT_CODE, ORIG_SERVICE_DATE order by null)
           as rnum -- is this necessary? Do we have duplicates?
       from clarity.CLARITY_TDL_TRAN trx
       join manuel.tmp_pat_enc_id_clarity2CDM encmap on trx.pat_enc_csn_id = encmap.encounterid_ide
       join pcori_cdm.encounter e on e.encounterid = encmap.encounterid
       where CPT_CODE is not null
           and trx.charge_slip_number is not null
           and trx.charge_slip_number not in (
               select charge_slip_number from tmp_bill_mrn_dup
               --  union --we aren't sure if this is necessary as you might have multiple office visits
                   --legitimately charged to all one visit, possibly. It doesn't seem as fishy as
                   --2 patients with the same slip number. 
               --select charge_slip_number from tmp_bill_enc_dup
               ) 
   ) where rnum = 1 and not PX_TYPE = 'ERROR'
);

--Clean up!
drop table tmp_bill_mrn_dup;
--drop table tmp_bill_enc_dup;

--professional billing
--insert into pcori_cdm.procedures(
   select 
      proceduresID,
      patid,
      encounterId,
      enc_type,
      admit_date,
      providerid,
      admit_date,
      PX,
      px_type,
      'BI' PX_Source,
      null raw_px,
      null raw_px_type
    from (
      select 
         proceduresid_seq.nextval proceduresID,
         e.patid,
         e.encounterId,
         e.enc_type,
         e.admit_date,
         e.providerid,
         e.admit_date,
         arpb_t.cpt_code as PX,
         case 
            when regexp_like(arpb_t.cpt_code,'^[0-9]{5}$') then 'C1' 
            when regexp_like(arpb_t.cpt_code,'^\+?[0-9]{4}[A-Z]$') then 'C2' --Or C3... 
                   --there is no way to tell without comparing it to the "current" list
                   --(note that stents could have  + to indicate additional ones.. -.-; 
            when regexp_like(arpb_t.cpt_code,'^[A-V][0-9]{4}$') then 'HC' 
                -- hcpcs- No level info in the link above
            else 'ERROR' end px_type
      from clarity.arpb_transactions arpb_t
       join manuel.tmp_pat_enc_id_clarity2CDM encmap on arpb_t.pat_enc_csn_id = encmap.encounterid_ide
       join pcori_cdm.encounter e on e.encounterid = encmap.encounterid
        /* We only want payments, not charges or adjustments
           ref: CLR203 Clarity Data Model - Resolute Professional Billing (3-9)
           "TX_TYPE_C - The type of this transaction.
                        1 - Charge
                        2 - Payment
                        3 - Adjustment" 
         */
      where arpb_t.tx_type_c = 1
      )
  where not px_type = 'ERROR';
);

create sequence pcori_cdm.prescribingID_seq as
 min 25376761
 max 999999999
 increment by 1
 start with 25376761
 cache 100;

--Medication
--insert into prescribing ( 
   select
     -- prescribing_seq.nextval prescribingID,
      e.patid,
      e.encounterId,
      e.providerid,
      TRUNC(com.ordering_date) + date_shift order_date,
      TO_CHAR (com.ordering_date, 'HH24:MI') order_time,
      case
         when com.order_start_time is not null then com.order_start_time 
         when com.start_date is not null then com.start_date 
         when com.order_inst is not null then order_inst 
         when com.ordering_date is not null then com.ordering_date 
       end start_date,
       com.order_end_time end_date,
       com.med_dis_disp_qty rx_quantity,
       com.refills rx_refills,
       null as RX_Days_Supply,
       case 
         when f.CDM_code is not null then f.CDM_code
         else 'NI'
       end  rx_frequency,
       'NI' as RX_Basis,
       case
         when rxn.rxnorm_code is not null then rxn.rxnorm_code
         else null 
       end rxnorm_cui,
       null raw_rx_med_name,
       null raw_tx_frequency,
       null raw_rxnorm_cui
   from
     CLARITY.order_med com
   join med_order_to_med_id otom on otom.order_med_id = com.order_med_id
   left join 
      MAPPING_FILES.CLARITY_FREQUENCY_TO_CDM@CLARITY_AT_EPIC.MI f 
            on com.hv_discr_freq_id = f.hv_discr_freq_id
   left join clarity.rxnorm_codes rxn on rxn.medication_id = com.medication_id
   join manuel.tmp_pat_enc_id_clarity2CDM encmap on com.pat_enc_csn_id = encmap.encounterid_ide
   join pcori_cdm.encounter e on e.encounterid = encmap.encounterid
);


CREATE SEQUENCE PCORI_CDM.LAB_RESULT_CM_SEQ
MIN VALUE 9999999
MAX VALUE 99999999999
START WITH 10000000
INCREMENT BY 1
CACHE 100;

--INSERT INTO LAB_RESULT_CM
SELECT  --PCORI_CDM.LAB_RESULT_CM_SEQ.NEXTVAL AS LAB_RESULT_CM_ID,
        ENC.PATID,
        ENC.ENCOUNTERID,
        LTC.TEST_NAME      AS LAB_NAME,
        'NI'               AS SPECIMEN_SOURCE,
        LTC.LOINCCODE      AS LAB_LOINC,
        'NI'               AS PRIORITY,
        'NI'               AS RESULT_LOC,
        NULL               AS LAB_PX,
        'LC'               AS LAB_PX_TYPE,
        COP.ORDERING_DATE  AS LAB_ORDER_DATE,
        TRUNC(COR.RESULT_DATE) AS SPECIMEN_DATE,
        TO_CHAR(COR.RESULT_DATE, 'HH24:MM' ) AS SPECIMEN_TIME,
        CASE 
            WHEN COR.COMP_OBS_INST_TM IS NOT NULL THEN TRUNC(COR.COMP_OBS_INST_TM)
            ELSE TRUNC(COR.RESULT_DATE)    
        END RESULT_DATE,
        CASE
           WHEN COR.COMP_OBS_INST_TM IS NOT NULL THEN TO_CHAR(COR.COMP_OBS_INST_TM, 'HH24:MM')
           ELSE TO_CHAR(COR.RESULT_DATE, 'HH24:MM')  
        END AS RESULT_TIME,
        CASE 
            WHEN COR.ORD_NUM_VALUE = 9999999 THEN 
                 CASE 
                     WHEN UPPER(COR.ORD_VALUE) = 'BORDERLINE' THEN 'BORDERLINE'
                     WHEN UPPER(COR.ORD_VALUE) = 'POSITIVE'   THEN 'POSITIVE'
                     WHEN UPPER(COR.ORD_VALUE) = 'NEGATIVE'   THEN 'NEGATIVE'
                     WHEN UPPER(COR.ORD_VALUE) = 'UNDETERMINED' THEN 'UNDETERMINED'
                     ELSE 'UN'
                 END
            ELSE 'NI'
        END RESULT_QUAL,
        CASE COR.ORD_NUM_VALUE
                WHEN 9999999 THEN NULL
                ELSE TO_CHAR(COR.ORD_NUM_VALUE)
        END RESULT_NUM,
        'NI',
        -- Replace units with PCORI standard units
        CASE
             WHEN INSTR(COR.REFERENCE_UNIT, 'cells')     > 0  THEN REGEXP_REPLACE(COR.REFERENCE_UNIT, 'cells', 'cell')
             WHEN INSTR(COR.REFERENCE_UNIT, 'Thousand' ) > 0  THEN REGEXP_REPLACE(COR.REFERENCE_UNIT, 'Thousand', 'K')
             WHEN INSTR(COR.REFERENCE_UNIT, 'Units' )    > 0  THEN REGEXP_REPLACE(COR.REFERENCE_UNIT, 'Units', 'U')
             WHEN INSTR(COR.REFERENCE_UNIT, 'MCG' )      > 0  THEN REGEXP_REPLACE(COR.REFERENCE_UNIT, 'MCG', 'UG')
             ELSE REFERENCE_UNIT
        END RESULT_UNIT,
        -- Remove math operators from COR.REFERENCE_LOW and replace with PCORI coding
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COR.REFERENCE_LOW, '<|>',''),'OR',''),'=','')) AS NORM_RANGE_LOW,
        CASE 
             WHEN REGEXP_LIKE (COR.REFERENCE_LOW, '> OR = *') THEN 'GE'
             WHEN REGEXP_LIKE (COR.REFERENCE_LOW, '< OR = *') THEN 'LE'
             WHEN REGEXP_LIKE (COR.REFERENCE_LOW, '<') THEN 'GT'
             WHEN REGEXP_LIKE (COR.REFERENCE_LOW, '>') THEN 'LT'
             ELSE 'EQ'
        END NORM_MODIFIER_LOW,
        -- Change non-numeric values for COR.REFERENCE_HIGH to numberic values
        CASE
                WHEN COR.REFERENCE_HIGH = '>60'      THEN '60'
                WHEN COR.REFERENCE_HIGH = '>=60'     THEN '60'
                WHEN COR.REFERENCE_HIGH = '>9Immune' THEN '9'
                WHEN COR.REFERENCE_HIGH = '<2.2'     THEN '2.2'
                WHEN COR.REFERENCE_HIGH = '<=60'     THEN '60'
                WHEN COR.REFERENCE_HIGH = '<100'     THEN '100'
                WHEN COR.REFERENCE_HIGH = '<=100'    THEN '100'
                ELSE COR.REFERENCE_HIGH
         END NORM_RANGE_HIGH,
        -- Remove math operators from COR.REFERENCE_HIGH and replace with PCORI coding
         CASE
                WHEN COR.REFERENCE_HIGH = '>60'      THEN 'GT'
                WHEN COR.REFERENCE_HIGH = '>=60'     THEN 'GE'
                WHEN COR.REFERENCE_HIGH = '>9Immune' THEN 'LT'
                WHEN COR.REFERENCE_HIGH = '<2.2'     THEN 'LT'
                WHEN COR.REFERENCE_HIGH = '<=60'     THEN 'LE'
                WHEN COR.REFERENCE_HIGH = '<100'     THEN 'LT'
                WHEN COR.REFERENCE_HIGH = '<=100'    THEN 'LR'
                WHEN COR.REFERENCE_HIGH IS NULL THEN 'NO'
                ELSE 'LE'
        END NORM_MODIFIER_HIGH,        
        CASE RESULT_FLAG_C
                WHEN 2    THEN 'AB'
                WHEN 5    THEN 'AH'
                WHEN 4    THEN 'AL'
                WHEN 7    THEN 'CH'
                WHEN 6    THEN 'CL'
                WHEN 3    THEN 'CR'
                WHEN 1    THEN 'NI'
                ELSE 'NI'
        END ABN_IND        
FROM    CLARITY.ORDER_RESULTS COR
INNER JOIN 
        CLARITY.ORDER_PROC COP
ON      COR.ORDER_PROC_ID = COP.ORDER_PROC_ID
INNER JOIN 
        CLARITY.PAT_ENC CPE 
ON      CPE.PAT_ENC_CSN_ID = COP.PAT_ENC_CSN_ID
INNER JOIN 
        I2B2METADATA2.LOINCTESTCODES LTC 
ON      TO_CHAR(COR.COMPONENT_ID) = LTC.TESTCODE
INNER JOIN
        MANUEL.ENC_ID_CLARITY2CDM ENCMAP
ON      COP.PAT_ENC_CSN_ID = ENCMAP.ENCOUNTERID_IDE
INNER JOIN
        PCORI_CDM.ENCOUNTER ENC
ON      ENCMAP.ENCOUNTERID = ENC.ENCOUNTERID
WHERE   COR.RESULT_STATUS_C = 3
        -- SELECT RESULT_STATUS_C FROM ZC_RESULT_STATUS WHERE ABBR='FINAL'
AND     COP.ORDER_TYPE_C IN (7, 61)
        -- SELECT ORDER_TYPE_C FROM ZC_ORDER_TYPE WHERE ABBR IN ('LAB', 'CONVERSION')
AND     COR.ORD_NUM_VALUE IS NOT NULL -- FILTERING FINAL MIGHT IMPLY THIS.
AND     ( COR.ORD_NUM_VALUE != 9999999 OR (ORD_NUM_VALUE = 9999999 AND UPPER (ORD_VALUE) IN ('BORDERLINE', 'POSITIVE', 'NEGATIVE', 'UNDETERMINED')) )
        -- This is to eliminate qualitiative lab results that do not have values 'BORDERLINE', 'POSITIVE', 'NEGATIVE', 'UNDETERMINED' 
        -- The qualitative values are uninterpretable


CREATE SEQUENCE PCORI_CDM.CONDITION_SEQ
MIN VALUE 10000
MAX VALUE 9999999999
START WITH 10000
INCREMENT BY 1
CACHE 100;


--INSERT INTO CONDITION
SELECT  --PCORI_CDM.CONDITION_SEQ.NEXTVAL AS CONDITIONID
        --ENC.PATID,
        --ENC.ENCOUNTERID,
        A.NOTED_DATE  + date_shift          AS REPORT_DATE,
        A.RESOLVED_DATE + date_shift         AS RESOLVE_DATE,
        NULL                    AS ONSET_DATE,
        CASE PROBLEM_STATUS_C
                WHEN 1 THEN 'AC'
                WHEN 2 THEN 'RS'
                ELSE 'NI'
        END CONDITION_STATUS,
        A.ICD9_CODE             AS CONDITION,
        '09'                    AS CONDITION_TYPE,
        'HC'                    AS CONDITION_SOURCE
FROM    CLARITY.PROBLEM_LIST A
INNER JOIN 
        MANUEL.ENC_ID_CLARITY2CDM  ENCMAP 
ON      A.PROBLEM_EPT_CSN  = ENCMAP.ENCOUNTERID_IDE
--INNER JOIN 
--        pcori_cdm.ENCOUNTER ENC
--ON      ENC.ENCOUNTERID = ENCMAP.ENCOUNTERID
join    manuel.Clarity_to_cdm_ptmap pmap
  on    pmap.patid_ide = a.pat_id
where PROBLEM_STATUS_C != 3
;
