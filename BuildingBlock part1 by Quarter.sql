SELECT
    main."Category",
    main."Year",
    main."Quarter",
    main."ClinicalRegion",
    main."Type",
    main.volume,
    -- main.currentquarterflag,
    CASE
        WHEN main."Type" = 'Actual' THEN 'dV Procedure Cases'
        WHEN main."Type" = 'Growth All' THEN NULL
        WHEN main."Type" = 'YoY Growth' THEN NULL
        WHEN main."Type" = 'Gynecology' THEN 'Actual with Gynecology for Subject'
        WHEN main."Type" = 'Hernia' THEN 'Actual with Inguinal Hernia and Ventral Hernia for Subject'
        WHEN main."Type" = 'Colorectal' THEN 'Actual with Rectal resection and Colon resection for Subject'
        WHEN main."Type" = 'Cholecystectomy' THEN 'Actual with Cholecystectomy for Subject'
        WHEN main."Type" = 'Growth Bucket as % of Total' THEN 'Actual with General Surgery, Thoracic, or Gynecology for Subject / Actual'
        WHEN main."Type" = 'Total Incremental Bubble+' THEN NULL
    END AS "Description"
FROM
    (
        SELECT
            'Procedure' AS "Category",
            s."Year",
            s."Quarter",
            s."ClinicalRegion",
            TO_DECIMAL(s."Actual", 15, 2) AS "Actual",
            TO_DECIMAL(s."Gynecology", 15, 2) AS "Gynecology",
            TO_DECIMAL(s."Hernia", 15, 2) AS "Hernia",
            TO_DECIMAL(s."Colorectal", 15, 2) AS "Colorectal",
            TO_DECIMAL(s."Cholecystectomy", 15, 2) AS "Cholecystectomy",
            TO_DECIMAL(s."Growth All", 15, 2) AS "Growth All",
            TO_DECIMAL(s."Growth Bucket as % of Total", 15, 2) AS "Growth Bucket as % of Total",
            -- YoY: lag 4분기, 연-분기 정렬키(yq_key)로 정렬
            TO_DECIMAL(
                CASE
                    WHEN LAG(s."Actual", 4) OVER (
                        PARTITION BY s."ClinicalRegion"
                        ORDER BY
                            s.yq_key
                    ) IS NULL THEN NULL
                    ELSE s."Actual" / NULLIF(
                        LAG(s."Actual", 4) OVER (
                            PARTITION BY s."ClinicalRegion"
                            ORDER BY
                                s.yq_key
                        ),
                        0
                    ) - 1
                END,
                15,
                2
            ) AS "YoY Growth" -- s.currentquarterflag  -- 필요하면 유지
        FROM
            (
                SELECT
                    p.year AS "Year",
                    p.quarter AS "Quarter",
                    p.ClinicalRegion AS "ClinicalRegion",
                    (p.year * 10 + p.quarter) AS yq_key,
                    -- 연-분기 순서키
                    p.currentquarterflag,
                    SUM(volume) AS "Actual",
                    SUM("Gynecology") AS "Gynecology",
                    SUM("Hernia") AS "Hernia",
                    SUM("Colorectal") AS "Colorectal",
                    SUM("Cholecystectomy") AS "Cholecystectomy",
                    SUM("Growth All") AS "Growth All",
                    SUM("Growth All") / NULLIF(SUM(volume), 0) AS "Growth Bucket as % of Total"
                FROM
                    (
                        SELECT
                            p.year,
                            p.quarter,
                            p.businesscategoryname,
                            p.subject,
                            p.ClinicalRegion,
                            p.currentquarterflag,
                            COUNT(p.recordid) AS volume,
                            CASE
                                WHEN CONCAT(p.businesscategoryname, p.subject) IN (
                                    'GynecologySacrocolpopexy',
                                    'GynecologyOvarian Cystectomy',
                                    'GynecologyOther Gynecology',
                                    'GynecologyOophorectomy',
                                    'GynecologyMyomectomy',
                                    'GynecologyHysterectomy - Malignant',
                                    'GynecologyHysterectomy - Benign',
                                    'GynecologyEndometriosis'
                                ) THEN COUNT(p.recordid)
                                ELSE NULL
                            END AS "Gynecology",
                            CASE
                                WHEN CONCAT(p.businesscategoryname, p.subject) IN (
                                    'General SurgeryVentral Hernia',
                                    'General SurgeryInguinal Hernia'
                                ) THEN COUNT(p.recordid)
                                ELSE NULL
                            END AS "Hernia",
                            CASE
                                WHEN CONCAT(p.businesscategoryname, p.subject) IN (
                                    'General SurgeryRectal Resection',
                                    'General SurgeryColon Resection'
                                ) THEN COUNT(p.recordid)
                                ELSE NULL
                            END AS "Colorectal",
                            CASE
                                WHEN CONCAT(p.businesscategoryname, p.subject) IN ('General SurgeryCholecystectomy') THEN COUNT(p.recordid)
                                ELSE NULL
                            END AS "Cholecystectomy",
                            CASE
                                WHEN CONCAT(p.businesscategoryname, p.subject) IN (
                                    'GynecologySacrocolpopexy',
                                    'GynecologyOvarian Cystectomy',
                                    'GynecologyOther Gynecology',
                                    'GynecologyOophorectomy',
                                    'GynecologyMyomectomy',
                                    'GynecologyHysterectomy - Malignant',
                                    'GynecologyHysterectomy - Benign',
                                    'GynecologyEndometriosis',
                                    'General SurgeryVentral Hernia',
                                    'General SurgeryInguinal Hernia',
                                    'General SurgeryRectal Resection',
                                    'General SurgeryColon Resection',
                                    'General SurgeryCholecystectomy'
                                ) THEN COUNT(p.recordid)
                                ELSE NULL
                            END AS "Growth All"
                        FROM
                            (
                                SELECT
                                    YEAR(p.proceduredatelocal) AS year,
                                    QUARTER(p.proceduredatelocal) AS quarter,
                                    p.businesscategoryname,
                                    p.subject,
                                    p.recordid,
                                    p.currentquarterflag,
                                    account.ClinicalRegion
                                FROM
                                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
                                WHERE
                                    p.status = 'Completed'
                                    AND account.clinicalregion = 'Asia : Korea'
                                    AND EXISTS (
                                        SELECT
                                            accountguid
                                        FROM
                                            "EDW"."MASTER"."VW_ACCOUNT" account
                                        WHERE
                                            p.accountguid = account.accountguid
                                            AND account.recordtype = 'Hospital'
                                    )
                            ) p
                        GROUP BY
                            p.year,
                            p.quarter,
                            p.businesscategoryname,
                            p.subject,
                            p.ClinicalRegion,
                            p.currentquarterflag
                    ) p
                GROUP BY
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.currentquarterflag
            ) s
    ) src UNPIVOT (
        volume FOR "Type" IN (
            "Actual",
            "Gynecology",
            "Hernia",
            "Colorectal",
            "Cholecystectomy",
            "Growth All",
            "Growth Bucket as % of Total",
            "YoY Growth"
        )
    ) AS main
UNION
ALL
SELECT
    'Procedure' AS "Category",
    bp.year AS "Year",
    bp.quarter AS "Quarter",
    bp.ClinicalRegion AS "ClinicalRegion",
    'Total Incremental Bubble+' AS "Type",
    TO_DECIMAL(
        bp."Bubble+" - LAG(bp."Bubble+", 4) OVER (
            PARTITION BY bp.ClinicalRegion
            ORDER BY
                (bp.year * 10 + bp.quarter)
        ),
        15,
        2
    ) AS "Volume",
    '' AS "Description"
FROM
    (
        SELECT
            bp.year,
            bp.quarter,
            bp.ClinicalRegion,
            bp.currentquarterflag,
            SUM(bp."Bubble+") AS "Bubble+"
        FROM
            (
                SELECT
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.surgeonguid,
                    p.currentquarterflag,
                    CASE
                        WHEN COUNT(p.recordid) >= 7 THEN 1
                        ELSE 0
                    END AS "Bubble+"
                FROM
                    (
                        SELECT
                            YEAR(p.proceduredatelocal) AS year,
                            QUARTER(p.proceduredatelocal) AS quarter,
                            p.surgeonguid,
                            p.recordid,
                            p.currentquarterflag,
                            account.ClinicalRegion
                        FROM
                            "EDW"."PROCEDURES"."VW_PROCEDURES" p
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
                        WHERE
                            p.status = 'Completed'
                            AND account.clinicalregion = 'Asia : Korea'
                            AND EXISTS (
                                SELECT
                                    accountguid
                                FROM
                                    "EDW"."MASTER"."VW_ACCOUNT" account
                                WHERE
                                    p.accountguid = account.accountguid
                                    AND account.recordtype = 'Hospital'
                            )
                    ) p
                GROUP BY
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.surgeonguid,
                    p.currentquarterflag
            ) bp
        GROUP BY
            bp.year,
            bp.quarter,
            bp.ClinicalRegion,
            bp.currentquarterflag
    ) bp
UNION
ALL -- % of Hospitals w/ Bubble+ Growth Surgeons = Growth Surgeon means who have Growth Procedure 
SELECT
    'Utilization' AS "Category",
    bp.year,
    bp.quarter,
    bp.ClinicalRegion,
    'Total Incremental Bubble+' AS "Type",
    TO_DECIMAL(
        COUNT(DISTINCT bp.accountguid) / NULLIF(a1."A_Account", 0),
        15,
        2
    ) AS "Volume",
    '% of Hospitals w/ Bubble+ Growth Surgeons = Growth Surgeon means who have Growth Procedure ' AS "Description"
FROM
    (
        SELECT
            bp.year,
            bp.quarter,
            bp.ClinicalRegion,
            -- bp.currentquarterflag,
            bp.accountguid,
            SUM(bp."Bubble+") AS "Bubble+"
        FROM
            (
                SELECT
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.accountguid,
                    p.surgeonguid,
                    p.currentquarterflag,
                    CASE
                        WHEN COUNT(p.recordid) >= 7 THEN 1
                        ELSE 0
                    END AS "Bubble+"
                FROM
                    (
                        SELECT
                            YEAR(p.proceduredatelocal) AS year,
                            QUARTER(p.proceduredatelocal) AS quarter,
                            p.accountguid,
                            p.surgeonguid,
                            p.recordid,
                            p.currentquarterflag,
                            account.ClinicalRegion
                        FROM
                            "EDW"."PROCEDURES"."VW_PROCEDURES" p
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
                        WHERE
                            p.status = 'Completed'
                            AND account.clinicalregion = 'Asia : Korea'
                            AND CONCAT(p.businesscategoryname, p.subject) IN (
                                'GynecologySacrocolpopexy',
                                'GynecologyOvarian Cystectomy',
                                'GynecologyOther Gynecology',
                                'GynecologyOophorectomy',
                                'GynecologyMyomectomy',
                                'GynecologyHysterectomy - Malignant',
                                'GynecologyHysterectomy - Benign',
                                'GynecologyEndometriosis',
                                'General SurgeryVentral Hernia',
                                'General SurgeryRectal Resection',
                                'General SurgeryInguinal Hernia',
                                'General SurgeryColon Resection',
                                'General SurgeryCholecystectomy'
                            )
                            AND EXISTS (
                                SELECT
                                    accountguid
                                FROM
                                    "EDW"."MASTER"."VW_ACCOUNT" account
                                WHERE
                                    p.accountguid = account.accountguid
                                    AND account.recordtype = 'Hospital'
                            )
                    ) p
                GROUP BY
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.accountguid,
                    p.surgeonguid,
                    p.currentquarterflag
            ) bp
        WHERE
            bp."Bubble+" >= 1
        GROUP BY
            bp.year,
            bp.quarter,
            bp.ClinicalRegion,
            bp.currentquarterflag,
            bp.accountguid
    ) bp
    LEFT JOIN (
        SELECT
            YEAR(p.proceduredatelocal) AS year,
            QUARTER(p.proceduredatelocal) AS quarter,
            COUNT(DISTINCT p.accountguid) AS "A_Account",
            account.ClinicalRegion
        FROM
            "EDW"."PROCEDURES"."VW_PROCEDURES" p
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
        WHERE
            p.status = 'Completed'
            AND account.clinicalregion = 'Asia : Korea'
            AND EXISTS (
                SELECT
                    accountguid
                FROM
                    "EDW"."MASTER"."VW_ACCOUNT" account
                WHERE
                    p.accountguid = account.accountguid
                    AND account.recordtype = 'Hospital'
            )
        GROUP BY
            YEAR(p.proceduredatelocal),
            QUARTER(p.proceduredatelocal),
            account.ClinicalRegion
    ) a1 ON a1.year = bp.year
    AND a1.quarter = bp.quarter
    AND a1.ClinicalRegion = bp.ClinicalRegion
GROUP BY
    bp.year,
    bp.quarter,
    bp.ClinicalRegion,
    a1."A_Account"
UNION
ALL -- Case per day by system
SELECT
    'Utilization' AS "Category",
    d.year AS "Year",
    d.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Case per day by system' AS "Type",
    TO_DECIMAL(AVG(d.cases_per_system_day), 15, 2) AS "Volume",
    'Overall mean across system-days' AS "Description"
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            pr.systemname,
            CAST(pr.localproceduredate AS DATE) AS proc_date,
            COUNT(*) AS cases_per_system_day
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
        GROUP BY
            1,
            2,
            3,
            4
    ) d
GROUP BY
    1,
    2,
    3,
    4,
    5
UNION
ALL -- System use days per week
SELECT
    'Utilization' AS "Category",
    EXTRACT(
        YEAR
        FROM
            w.week_start
    ) AS "Year",
    EXTRACT(
        QUARTER
        FROM
            w.week_start
    ) AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'System use days per week' AS "Type",
    /* system-week 행들의 평균 → 가중 불필요, 평균의 평균 방지 */
    TO_DECIMAL(AVG(w.used_days_in_week), 15, 2) AS "Volume",
    'System use days per week (country avg across system-weeks)' AS "Description"
FROM
    (
        /* 1) system-week 단위로 '사용된 일수'를 계산
         - 같은 주 내에서 날짜별로 케이스가 1건 이상 있으면 그 '날'은 1일로 카운트 */
        SELECT
            pr.systemname,
            /* Snowflake: DATE_TRUNC('week', ...) 는 주의 시작일(보통 월요일)로 절단 */
            DATE_TRUNC('week', pr.localproceduredate) :: DATE AS week_start,
            COUNT(DISTINCT CAST(pr.localproceduredate AS DATE)) AS used_days_in_week
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
        GROUP BY
            1,
            2
    ) w
GROUP BY
    1,
    2,
    3,
    4,
    5,
    7
UNION
ALL -- Case per day by Surgeon
SELECT
    'Utilization' AS "Category",
    d.year AS "Year",
    d.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Case per day by Surgeon' AS "Type",
    TO_DECIMAL(AVG(d.cases_per_surgeon_day), 15, 2) AS "Volume",
    'Avg cases per surgeon-day; country avg' AS "Description"
FROM
    (
        /* 1) surgeon × day 레벨로, 하루 케이스 수를 계산 */
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            pr.accountguid,
            pr.accountid,
            pr.accountname,
            pr.surgeonguid,
            pr.surgeonid,
            CAST(pr.localproceduredate AS DATE) AS proc_date,
            COUNT(*) AS cases_per_surgeon_day -- 필요 시 COUNT(DISTINCT pr.casenumber)로 대체
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
        GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8
    ) d
GROUP BY
    1,
    2,
    3,
    4,
    5,
    7
UNION
ALL
SELECT
    'Utilization' AS "Category",
    dtf.year,
    dtf.quarter,
    dtf.ClinicalRegion,
    'Average Days to First Case after Installation' AS "Type",
    to_decimal(AVG(dtf.daystofirstcase), 15, 2) as volume,
    'Average days between first case in account and finance install date (excluded if finance install date is after the first case in account) for years of finance install date' as description
FROM
    (
        SELECT
            year(p.installdate) as year,
            quarter(p.installdate) as quarter,
            p.ClinicalRegion,
            p.proceduredatelocal,
            p.installedbaseguid,
            p.installdate,
            p.recordid,
            p.rownum,
            datediff(day, p.installdate, p.proceduredatelocal) as daystofirstcase
        FROM
            (
                SELECT
                    year(p.proceduredatelocal) as year,
                    quarter(p.proceduredatelocal) as quarter,
                    account.ClinicalRegion,
                    p.proceduredatelocal,
                    p.installedbaseguid,
                    p.recordid,
                    account.accountname,
                    installedbase.installdate,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.installedbaseguid
                        ORDER BY
                            p.proceduredatelocal
                    ) as rownum
                FROM
                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
                    LEFT JOIN "EDW"."MASTER"."VW_INSTALLBASE" installedbase ON installedbase.installbaseguid = p.installedbaseguid
                WHERE
                    p.status = 'Completed'
                    and account.clinicalregion = 'Asia : Korea'
                    AND EXISTS(
                        SELECT
                            accountguid
                        FROM
                            "EDW"."MASTER"."VW_ACCOUNT" account
                        WHERE
                            p.accountguid = account.accountguid
                            AND account.recordtype = 'Hospital'
                    )
            ) p
        WHERE
            p.rownum = 1
            and year(p.installdate) >= 2015
            and datediff(day, p.installdate, p.proceduredatelocal) >= 0
    ) dtf
GROUP BY
    dtf.year,
    dtf.quarter,
    dtf.ClinicalRegion
union
all
SELECT
    'Utilization' AS "Category",
    TO_NUMBER(LEFT(main.quarter, 4)) AS "Year",
    -- INT
    TO_NUMBER(RIGHT(main.quarter, 1)) AS "Quarter",
    -- INT 1–4
    'Asia : Korea' AS "ClinicalRegion",
    'Average Utilization' AS "Type",
    TO_DECIMAL(
        SUM(main."cq_rr") / NULLIF(SUM(main."pq_ib"), 0),
        15,
        2
    ) AS "Volume",
    'Average Utilization for all Active Account' AS "Description"
FROM
    (
        SELECT
            ut.quarter,
            SUM(ut.pq_ib) AS "pq_ib",
            SUM(ut.cq_ib) AS "cq_ib",
            SUM(ut.cq_rr) AS "cq_rr",
            CASE
                WHEN LEFT(ut.quarter, 4) = LEFT(ut."InstalldateQ", 4) THEN 'Installed_That_Year'
                WHEN LEFT(ut.quarter, 4) > LEFT(ut."InstalldateQ", 4) THEN 'Installed_UntilPY'
            END AS "Flag"
        FROM
            EDWLABS.SALESLAB.KOREAACCOUNTUTILIZATION ut
        WHERE
            TO_NUMBER(LEFT(ut.quarter, 4)) > 2015
        GROUP BY
            ut.quarter,
            ut."InstalldateQ"
    ) main
GROUP BY
    1,
    2,
    3,
    4,
    5,
    7
union
all
SELECT
    'Utilization' AS "Category",
    TO_NUMBER(LEFT(main.quarter, 4)) AS "Year",
    -- INT
    TO_NUMBER(RIGHT(main.quarter, 1)) AS "Quarter",
    -- INT 1–4
    'Asia : Korea' AS "ClinicalRegion",
    'Average Utilization_Tier1-A' AS "Type",
    TO_DECIMAL(
        SUM(main."cq_rr") / NULLIF(SUM(main."pq_ib"), 0),
        15,
        2
    ) AS "Volume",
    'Average Utilization for all Active Account' AS "Description"
FROM
    (
        SELECT
            ut.quarter,
            SUM(ut.pq_ib) AS "pq_ib",
            SUM(ut.cq_ib) AS "cq_ib",
            SUM(ut.cq_rr) AS "cq_rr",
            CASE
                WHEN LEFT(ut.quarter, 4) = LEFT(ut."InstalldateQ", 4) THEN 'Installed_That_Year'
                WHEN LEFT(ut.quarter, 4) > LEFT(ut."InstalldateQ", 4) THEN 'Installed_UntilPY'
            END AS "Flag"
        FROM
            EDWLABS.SALESLAB.KOREAACCOUNTUTILIZATION ut
            INNER JOIN (
                SELECT
                    ACCOUNTGUID,
                    tier
                FROM
                    EDWLABS.SALESLAB.KOREA_TIERSEGMANTATION
                WHERE
                    YEAR = 2025
            ) tier ON ut.ACCOUNTGUID = tier.ACCOUNTGUID
        WHERE
            TO_NUMBER(LEFT(ut.quarter, 4)) > 2015
            AND LEFT(tier.tier, 7) = 'Tier1-A'
        GROUP BY
            ut.quarter,
            ut."InstalldateQ"
    ) main
GROUP BY
    1,
    2,
    3,
    4,
    5,
    7
UNION
all
SELECT
    'Utilization' AS "Category",
    TO_NUMBER(LEFT(main.quarter, 4)) AS "Year",
    -- INT
    TO_NUMBER(RIGHT(main.quarter, 1)) AS "Quarter",
    -- INT 1–4
    'Asia : Korea' AS "ClinicalRegion",
    'Average Utilization_Tier1-B' AS "Type",
    TO_DECIMAL(
        SUM(main."cq_rr") / NULLIF(SUM(main."pq_ib"), 0),
        15,
        2
    ) AS "Volume",
    'Average Utilization for all Active Account' AS "Description"
FROM
    (
        SELECT
            ut.quarter,
            SUM(ut.pq_ib) AS "pq_ib",
            SUM(ut.cq_ib) AS "cq_ib",
            SUM(ut.cq_rr) AS "cq_rr",
            CASE
                WHEN LEFT(ut.quarter, 4) = LEFT(ut."InstalldateQ", 4) THEN 'Installed_That_Year'
                WHEN LEFT(ut.quarter, 4) > LEFT(ut."InstalldateQ", 4) THEN 'Installed_UntilPY'
            END AS "Flag"
        FROM
            EDWLABS.SALESLAB.KOREAACCOUNTUTILIZATION ut
            INNER JOIN (
                SELECT
                    ACCOUNTGUID,
                    tier
                FROM
                    EDWLABS.SALESLAB.KOREA_TIERSEGMANTATION
                WHERE
                    YEAR = 2025
            ) tier ON ut.ACCOUNTGUID = tier.ACCOUNTGUID
        WHERE
            TO_NUMBER(LEFT(ut.quarter, 4)) > 2015
            AND LEFT(tier.tier, 7) = 'Tier1-B'
        GROUP BY
            ut.quarter,
            ut."InstalldateQ"
    ) main
GROUP BY
    1,
    2,
    3,
    4,
    5,
    7
UNION
all
select
    'Utilization' AS "Category",
    LEFT(main.quarter, 4) AS year,
    RIGHT(main.quarter, 1) AS quarter,
    'Asia : Korea' AS ClinicalRegion,
    'Average Utilization_Tier2&3' AS "Type",
    to_decimal(SUM("cq_rr") / SUM("pq_ib"), 15, 2) as volume,
    'Average Utilization for all Active Account' as description
FROM
    (
        SELECT
            ut.quarter,
            SUM(ut.pq_ib) AS "pq_ib",
            SUM(ut.cq_ib) AS "cq_ib",
            SUM(ut.cq_rr) AS "cq_rr",
            CASE
                WHEN LEFT(ut.quarter, 4) = LEFT(ut."InstalldateQ", 4) THEN 'Installed_That_Year'
                WHEN LEFT(ut.quarter, 4) > LEFT(ut."InstalldateQ", 4) THEN 'Installed_UntilPY'
            END AS "Flag"
        FROM
            EDWLABS.SALESLAB.KOREAACCOUNTUTILIZATION ut
            INNER JOIN (
                SELECT
                    ACCOUNTGUID,
                    tier
                FROM
                    EDWLABS.SALESLAB.KOREA_TIERSEGMANTATION
                WHERE
                    YEAR = 2025
            ) tier ON ut.ACCOUNTGUID = tier.ACCOUNTGUID
        WHERE
            LEFT(ut.quarter, 4) > 2015
            AND (
                LEFT(tier.tier, 6) = 'Tier 2'
                OR LEFT(tier.tier, 6) = 'Tier 3'
            )
        GROUP BY
            ut.quarter,
            ut."InstalldateQ"
    ) main
GROUP BY
    LEFT(main.quarter, 4),
    RIGHT(main.quarter, 1)
union
all -- Average Utilization — Systems Installed Until Prior Year
SELECT
    'Utilization' AS "Category",
    TO_NUMBER(LEFT(main.quarter, 4)) AS "Year",
    TO_NUMBER(RIGHT(main.quarter, 1)) AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Average Utilization' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN main."Flag" = 'Installed_UntilPY' THEN main."cq_rr"
            END
        ) / NULLIF(
            SUM(
                CASE
                    WHEN main."Flag" = 'Installed_UntilPY' THEN main."cq_ib"
                END
            ),
            0
        ),
        15,
        2
    ) AS "Volume",
    'Average Utilization for System Until Prior year' AS "Description"
FROM
    (
        SELECT
            ut.quarter,
            SUM(ut.pq_ib) AS "pq_ib",
            SUM(ut.cq_ib) AS "cq_ib",
            SUM(ut.cq_rr) AS "cq_rr",
            CASE
                WHEN LEFT(ut.quarter, 4) = LEFT(ut."InstalldateQ", 4) THEN 'Installed_That_Year'
                WHEN LEFT(ut.quarter, 4) > LEFT(ut."InstalldateQ", 4) THEN 'Installed_UntilPY'
            END AS "Flag"
        FROM
            EDWLABS.SALESLAB.KOREAACCOUNTUTILIZATION ut
        WHERE
            LEFT(ut.quarter, 4) > '2015'
        GROUP BY
            ut.quarter,
            ut."InstalldateQ"
    ) main
GROUP BY
    TO_NUMBER(LEFT(main.quarter, 4)),
    TO_NUMBER(RIGHT(main.quarter, 1))
union
all --Utilization - System installed that year
SELECT
    'Utilization' AS "Category",
    TO_NUMBER(LEFT(main.quarter, 4)) AS "Year",
    -- INT
    TO_NUMBER(RIGHT(main.quarter, 1)) AS "Quarter",
    -- INT 1–4
    'Asia : Korea' AS "ClinicalRegion",
    'Average Utilization' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN "Flag" = 'Installed_That_Year' THEN main."cq_rr"
            END
        ) / NULLIF(
            SUM(
                CASE
                    WHEN "Flag" = 'Installed_That_Year' THEN main."cq_ib"
                END
            ),
            0
        ),
        15,
        2
    ) AS "Volume",
    'Average Utilization for System installed That year' AS "Description"
FROM
    (
        SELECT
            ut.quarter,
            SUM(ut.pq_ib) AS "pq_ib",
            SUM(ut.cq_ib) AS "cq_ib",
            SUM(ut.cq_rr) AS "cq_rr",
            CASE
                WHEN LEFT(ut.quarter, 4) = LEFT(ut."InstalldateQ", 4) THEN 'Installed_That_Year'
                WHEN LEFT(ut.quarter, 4) > LEFT(ut."InstalldateQ", 4) THEN 'Installed_UntilPY'
            END AS "Flag"
        FROM
            EDWLABS.SALESLAB.KOREAACCOUNTUTILIZATION ut
        WHERE
            TO_NUMBER(LEFT(ut.quarter, 4)) > 2015
        GROUP BY
            ut.quarter,
            ut."InstalldateQ"
    ) main
GROUP BY
    1,
    2,
    3,
    4,
    5,
    7
union
all -- Surgeon Part
SELECT
    main."Category",
    main."Year",
    main."Quarter",
    main."ClinicalRegion",
    main."Type",
    main."Volume",
    'Surgeon information' AS "Description"
FROM
    (
        SELECT
            'Surgeon' AS "Category",
            YEAR(P.localproceduredate) AS "Year",
            QUARTER(P.localproceduredate) AS "Quarter",
            P.clinicalregion AS "ClinicalRegion",
            TO_DECIMAL(COUNT(DISTINCT P.surgeonguid), 15, 2) AS "Active Surgeons",
            TO_DECIMAL(COUNT(DISTINCT P.casenumber), 15, 2) AS "Procedures",
            TO_DECIMAL(
                COUNT(DISTINCT P.casenumber) / NULLIF(COUNT(DISTINCT P.surgeonguid), 0),
                15,
                2
            ) AS "ProcedurePerSurgeon",
            /* ▼ 여기만 변경: a2 값을 집계로 감싸서 GROUP BY 위반 해소 */
            TO_DECIMAL(MAX(COALESCE(a2."NewSurgeon", 0)), 15, 2) AS "NewSurgeon"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY P
            LEFT JOIN (
                SELECT
                    YEAR(P.localproceduredate) AS "Year",
                    QUARTER(P.localproceduredate) AS "Quarter",
                    COUNT(DISTINCT P.surgeonguid) AS "NewSurgeon"
                FROM
                    EDW.PROCEDURES.VW_PROCEDURESUMMARY P
                WHERE
                    P.totalsurgeoncaserank = 1
                    AND P.clinicalregion = 'Asia : Korea'
                GROUP BY
                    YEAR(P.localproceduredate),
                    QUARTER(P.localproceduredate)
            ) a2 ON a2."Year" = YEAR(P.localproceduredate)
            AND a2."Quarter" = QUARTER(P.localproceduredate)
        WHERE
            P.clinicalregion = 'Asia : Korea'
            AND P.casestatus = 'Completed'
        GROUP BY
            YEAR(P.localproceduredate),
            QUARTER(P.localproceduredate),
            P.clinicalregion
    ) src UNPIVOT (
        "Volume" FOR "Type" IN (
            "Active Surgeons",
            "Procedures",
            "ProcedurePerSurgeon",
            "NewSurgeon"
        )
    ) AS main
union
all
SELECT
    'Team Contribution' AS "Category",
    TO_NUMBER(year) AS "Year",
    TO_NUMBER(quarter) AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    main."Type",
    main."Volume",
    'CSR Basic Training Contribution' AS "Description"
FROM
    (
        SELECT
            c.year,
            c.quarter,
            TO_DECIMAL(
                COUNT(
                    DISTINCT CASE
                        WHEN c.volume >= 1 THEN c.histcsr
                    END
                ) / NULLIF(
                    COUNT(
                        DISTINCT CASE
                            WHEN c.volume >= 0 THEN c.histcsr
                        END
                    ),
                    0
                ),
                15,
                2
            ) AS "%ofContributionCSR"
        FROM
            (
                SELECT
                    csr.accountguid,
                    csr.year,
                    csr.quarter,
                    csr.histcsr,
                    COALESCE(tr100.volume, 0) AS volume
                FROM
                    (
                        SELECT
                            DISTINCT ha.accountguid,
                            SUBSTRING(ha.yearquarter, 1, 4) AS year,
                            -- 문자열 'YYYY'
                            RIGHT(ha.yearquarter, 1) AS quarter,
                            -- 문자열 'n'
                            ha.histcsr,
                            account.ClinicalRegion
                        FROM
                            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
                        WHERE
                            account.clinicalregion = 'Asia : Korea'
                            AND ha.histcsr IS NOT NULL
                    ) csr
                    LEFT JOIN (
                        SELECT
                            YEAR(tr.CERTIFICATIONDATE) AS year,
                            -- INT
                            QUARTER(tr.CERTIFICATIONDATE) AS quarter,
                            -- INT
                            account.ClinicalRegion,
                            contact.accountguid,
                            COUNT(DISTINCT tr.contact) AS volume
                        FROM
                            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
                            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
                        WHERE
                            tr.eventtype IN (
                                'Technology Training Multi-Port',
                                'Technology Training Single-Port'
                            )
                            AND tr.courseid = 'TR100'
                            AND tr.CERTIFICATIONDATE IS NOT NULL
                            AND account.ClinicalRegion = 'Asia : Korea'
                            AND (
                                tr.role = 'Surgeon'
                                OR tr.role IS NULL
                            )
                        GROUP BY
                            YEAR(tr.CERTIFICATIONDATE),
                            QUARTER(tr.CERTIFICATIONDATE),
                            account.ClinicalRegion,
                            contact.accountguid
                    ) tr100 ON TO_NUMBER(csr.year) = tr100.year
                    AND TO_NUMBER(csr.quarter) = tr100.quarter
                    AND csr.accountguid = tr100.accountguid
            ) c
        WHERE
            c.histcsr NOT LIKE '%Open%'
        GROUP BY
            c.year,
            c.quarter
    ) base UNPIVOT (
        "Volume" FOR "Type" IN ("%ofContributionCSR")
    ) AS main
union
all
SELECT
    'Team Contribution' AS "Category",
    TO_NUMBER(main.year) AS "Year",
    TO_NUMBER(main.quarter) AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    main."Type",
    main."Volume",
    'CSR Bubble+ Contribution' AS "Description"
FROM
    (
        /* Bubble+ 기여율 산출(분자/분모는 분기 단위) */
        SELECT
            c.year,
            c.quarter,
            TO_DECIMAL(
                COUNT(
                    DISTINCT CASE
                        WHEN c.volume >= 1 THEN c.histcsr
                    END
                ) / NULLIF(
                    COUNT(
                        DISTINCT CASE
                            WHEN c.volume >= 0 THEN c.histcsr
                        END
                    ),
                    0
                ),
                15,
                2
            ) AS "%BBContri_CSR"
        FROM
            (
                /* 계정×분기: CSR 보유 이력 + TR100 Bubble+ 달성 여부 결합 */
                SELECT
                    csr.accountguid,
                    csr.year,
                    csr.quarter,
                    csr.histcsr,
                    COALESCE(tr100.volume, 0) AS volume -- 계정×분기 Bubble+ 컨택 수
                FROM
                    (
                        /* CSR 히스토리 (연·분기 문자열) */
                        SELECT
                            DISTINCT ha.accountguid,
                            SUBSTRING(ha.yearquarter, 1, 4) AS year,
                            -- 'YYYY'
                            RIGHT(ha.yearquarter, 1) AS quarter,
                            -- 'n'
                            ha.histcsr,
                            account.ClinicalRegion
                        FROM
                            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
                        WHERE
                            account.clinicalregion = 'Asia : Korea'
                            AND ha.histcsr IS NOT NULL
                    ) csr
                    LEFT JOIN (
                        /* TR100 수료 후 90일 Bubble+ 달성 컨택 집계 (이벤트타입별 임계점 분리) */
                        SELECT
                            YEAR(tr.CERTIFICATIONDATE) AS year,
                            QUARTER(tr.CERTIFICATIONDATE) AS quarter,
                            account.ClinicalRegion,
                            contact.accountguid,
                            COUNT(DISTINCT tr.contact) AS volume
                        FROM
                            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
                            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
                            LEFT JOIN EDW.TRAINING.VW_TRAINING tra ON tr.recordid = tra.trainingguid
                        WHERE
                            tr.eventtype IN (
                                'Technology Training Multi-Port',
                                'Technology Training Single-Port'
                            )
                            AND tr.courseid = 'TR100'
                            AND tr.CERTIFICATIONDATE IS NOT NULL
                            AND account.ClinicalRegion = 'Asia : Korea'
                            AND (
                                tr.role = 'Surgeon'
                                OR tr.role IS NULL
                            )
                            AND (
                                (
                                    tr.eventtype = 'Technology Training Multi-Port'
                                    AND tra.casesin90 >= 7
                                )
                                OR (
                                    tr.eventtype = 'Technology Training Single-Port'
                                    AND tra.spcasesin90 >= 7
                                )
                            )
                        GROUP BY
                            YEAR(tr.CERTIFICATIONDATE),
                            QUARTER(tr.CERTIFICATIONDATE),
                            account.ClinicalRegion,
                            contact.accountguid
                    ) tr100 ON TO_NUMBER(csr.year) = tr100.year
                    AND TO_NUMBER(csr.quarter) = tr100.quarter
                    AND csr.accountguid = tr100.accountguid
            ) c
        WHERE
            c.histcsr NOT LIKE '%Open%'
        GROUP BY
            c.year,
            c.quarter
    ) base UNPIVOT ("Volume" FOR "Type" IN ("%BBContri_CSR")) AS main
union
all
SELECT
    'Team Contribution' AS "Category",
    TO_NUMBER(main.year) AS "Year",
    TO_NUMBER(main.quarter) AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    main."Type",
    main."Volume",
    'CSR Sustainable+ Contribution' AS "Description"
FROM
    (
        /* 분기별 Sustainable+ 기여율 계산 */
        SELECT
            c.year,
            c.quarter,
            TO_DECIMAL(
                COUNT(
                    DISTINCT CASE
                        WHEN c.volume >= 1 THEN c.histcsr
                    END
                ) / NULLIF(
                    COUNT(
                        DISTINCT CASE
                            WHEN c.volume >= 0 THEN c.histcsr
                        END
                    ),
                    0
                ),
                15,
                2
            ) AS "%SSContri_CSR"
        FROM
            (
                /* 계정×분기: CSR 보유 이력 + TR100 Sustainable+ 달성 여부 결합 */
                SELECT
                    csr.accountguid,
                    csr.year,
                    csr.quarter,
                    csr.histcsr,
                    COALESCE(tr100.volume, 0) AS volume -- 계정×분기 Sustainable+ 컨택 수
                FROM
                    (
                        /* CSR 히스토리 (연·분기 문자열) */
                        SELECT
                            DISTINCT ha.accountguid,
                            SUBSTRING(ha.yearquarter, 1, 4) AS year,
                            -- 'YYYY'
                            RIGHT(ha.yearquarter, 1) AS quarter,
                            -- 'n'
                            ha.histcsr,
                            account.ClinicalRegion
                        FROM
                            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
                        WHERE
                            account.clinicalregion = 'Asia : Korea'
                            AND ha.histcsr IS NOT NULL
                    ) csr
                    LEFT JOIN (
                        /* TR100 수료 후 90일 Sustainable+ 달성 컨택 집계 (이벤트타입별 임계점 분리: 13+) */
                        SELECT
                            YEAR(tr.CERTIFICATIONDATE) AS year,
                            QUARTER(tr.CERTIFICATIONDATE) AS quarter,
                            account.ClinicalRegion,
                            contact.accountguid,
                            COUNT(DISTINCT tr.contact) AS volume
                        FROM
                            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
                            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
                            LEFT JOIN EDW.TRAINING.VW_TRAINING tra ON tr.recordid = tra.trainingguid
                        WHERE
                            tr.eventtype IN (
                                'Technology Training Multi-Port',
                                'Technology Training Single-Port'
                            )
                            AND tr.courseid = 'TR100'
                            AND tr.CERTIFICATIONDATE IS NOT NULL
                            AND account.ClinicalRegion = 'Asia : Korea'
                            AND (
                                tr.role = 'Surgeon'
                                OR tr.role IS NULL
                            )
                            AND (
                                (
                                    tr.eventtype = 'Technology Training Multi-Port'
                                    AND tra.casesin90 >= 13
                                )
                                OR (
                                    tr.eventtype = 'Technology Training Single-Port'
                                    AND tra.spcasesin90 >= 13
                                )
                            )
                        GROUP BY
                            YEAR(tr.CERTIFICATIONDATE),
                            QUARTER(tr.CERTIFICATIONDATE),
                            account.ClinicalRegion,
                            contact.accountguid
                    ) tr100 ON TO_NUMBER(csr.year) = tr100.year
                    AND TO_NUMBER(csr.quarter) = tr100.quarter
                    AND csr.accountguid = tr100.accountguid
            ) c
        WHERE
            c.histcsr NOT LIKE '%Open%'
        GROUP BY
            c.year,
            c.quarter
    ) base UNPIVOT ("Volume" FOR "Type" IN ("%SSContri_CSR")) AS main
union
all
/* Incr Procedures per QBR Actual = (YoY 증감 케이스 수) / (# of QBR) */
SELECT
    'Team Contribution' AS "Category",
    p.year AS "Year",
    p.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Incr Procedures per QBR Actual' AS "Type",
    TO_DECIMAL(
        -- 분모 0 방지: 원래 로직 유지(0이면 분자 그대로)
        CASE
            WHEN qbr.qbr_cnt = 0
            OR qbr.qbr_cnt IS NULL THEN p.yoy_incr
            ELSE p.yoy_incr / qbr.qbr_cnt
        END,
        15,
        2
    ) AS "Volume",
    'YoY Procedure / # QBR' AS "Description"
FROM
    (
        /* 연·분기별 절대 케이스 수 → 전년동기 대비 증감 계산 */
        SELECT
            x.year,
            x.quarter,
            x.ClinicalRegion,
            /* 전년동기 대비 증감: (올해 분기 케이스) - (작년 동일 분기 케이스) */
            (
                x.proc_cnt - LAG(x.proc_cnt, 4) OVER (
                    PARTITION BY x.ClinicalRegion
                    ORDER BY
                        (x.year * 10 + x.quarter) -- 시간 정렬 안정화
                )
            ) AS yoy_incr
        FROM
            (
                SELECT
                    YEAR(p.proceduredatelocal) AS year,
                    QUARTER(p.proceduredatelocal) AS quarter,
                    account.ClinicalRegion AS ClinicalRegion,
                    COUNT(p.recordid) AS proc_cnt
                FROM
                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
                WHERE
                    p.status = 'Completed'
                    AND account.ClinicalRegion = 'Asia : Korea'
                    AND EXISTS (
                        SELECT
                            1
                        FROM
                            "EDW"."MASTER"."VW_ACCOUNT" a2
                        WHERE
                            a2.accountguid = p.accountguid
                            AND a2.recordtype = 'Hospital'
                    )
                GROUP BY
                    YEAR(p.proceduredatelocal),
                    QUARTER(p.proceduredatelocal),
                    account.ClinicalRegion
            ) x
    ) p
    LEFT JOIN (
        /* 분기별 QBR 카운트 */
        SELECT
            TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
            -- 'YYYY' → INT
            TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
            -- 'n'    → INT
            account.ClinicalRegion AS ClinicalRegion,
            COUNT(DISTINCT ha.histcsr) AS qbr_cnt
        FROM
            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
        WHERE
            LOWER(ha.histcsr) NOT LIKE '%open%'
            AND account.ClinicalRegion = 'Asia : Korea'
        GROUP BY
            TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)),
            TO_NUMBER(RIGHT(ha.yearquarter, 1)),
            account.ClinicalRegion
    ) qbr ON qbr.year = p.year
    AND qbr.quarter = p.quarter
    AND qbr.ClinicalRegion = p.ClinicalRegion
union
all
SELECT
    'Team Contribution' AS "Category",
    tr100.year AS "Year",
    tr100.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Basic Trainings' AS "Type",
    TO_DECIMAL(COUNT(DISTINCT tr100.contactguid), 15, 2) AS "Volume",
    '# of TR100' AS "Description"
FROM
    (
        SELECT
            YEAR(tr.CERTIFICATIONDATE) AS year,
            QUARTER(tr.CERTIFICATIONDATE) AS quarter,
            tr.contact AS contactguid,
            account.ClinicalRegion
        FROM
            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
        WHERE
            tr.eventtype IN (
                'Technology Training Multi-Port',
                'Technology Training Single-Port'
            ) -- 필요 시 Single-Port도 추가 가능
            AND tr.courseid = 'TR100'
            AND tr.CERTIFICATIONDATE IS NOT NULL
            AND account.ClinicalRegion = 'Asia : Korea'
            AND (
                tr.role = 'Surgeon'
                OR tr.role IS NULL
            )
    ) tr100
GROUP BY
    tr100.year,
    tr100.quarter
union
all
/* Basic Training Productivity (90 Day)
 - 최하위 레벨: tr.RECORDID (개별 Training)
 - 분자: recordid별 delta_90d 합 (MP는 cases*, SP는 spcases*)
 - 분모: DISTINCT recordid (Training 개수)
 - 출력: 표준 7컬럼 (UNION ALL 체인 호환)
 */
SELECT
    'Team Contribution' AS "Category",
    b.year AS "Year",
    b.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Basic Training Productivity (90 Day)' AS "Type",
    TO_DECIMAL(
        SUM(b.delta_90d) / NULLIF(COUNT(DISTINCT b.recordid), 0),
        15,
        2
    ) AS "Volume",
    'Region-level productivity using MP/SP deltas; denominator = distinct training (recordid)' AS "Description"
FROM
    (
        SELECT
            tr.recordid,
            YEAR(tr.CERTIFICATIONDATE) AS year,
            QUARTER(tr.CERTIFICATIONDATE) AS quarter,
            tr.eventtype,
            tr.contact AS contactguid,
            /* recordid(Training) 단위 delta 계산: 이벤트타입별로 해당 컬럼만 사용 */
            CASE
                WHEN tr.eventtype = 'Technology Training Multi-Port' THEN COALESCE(tra.casesin90, 0) - COALESCE(tra.casespre90, 0)
                WHEN tr.eventtype = 'Technology Training Single-Port' THEN COALESCE(tra.spcasesin90, 0) - COALESCE(tra.spcasespre90, 0)
                ELSE 0
            END AS delta_90d
        FROM
            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
            LEFT JOIN EDW.TRAINING.VW_TRAINING tra ON tr.recordid = tra.trainingguid
        WHERE
            tr.eventtype IN (
                'Technology Training Multi-Port',
                'Technology Training Single-Port'
            )
            AND tr.courseid = 'TR100'
            AND tr.CERTIFICATIONDATE IS NOT NULL
            AND account.ClinicalRegion = 'Asia : Korea'
            AND (
                tr.role = 'Surgeon'
                OR tr.role IS NULL
            )
    ) b
GROUP BY
    b.year,
    b.quarter
union
all
SELECT
    'Team Contribution' AS "Category",
    b.year AS "Year",
    b.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Bubble+ Training Rate' AS "Type",
    TO_DECIMAL(SUM(b.is_bubble) / NULLIF(COUNT(*), 0), 15, 2) AS "Volume",
    '% of completed Basic Trainings which resulted in 7+ cases in 90 days' AS "Description"
FROM
    (
        SELECT
            DISTINCT YEAR(tr.CERTIFICATIONDATE) AS year,
            QUARTER(tr.CERTIFICATIONDATE) AS quarter,
            tr.recordid,
            tr.contact AS contactguid,
            account.ClinicalRegion,
            CASE
                WHEN tr.eventtype = 'Technology Training Multi-Port'
                AND COALESCE(tra.casesin90, 0) >= 7 THEN 1
                WHEN tr.eventtype = 'Technology Training Single-Port'
                AND COALESCE(tra.spcasesin90, 0) >= 7 THEN 1
                ELSE 0
            END AS is_bubble
        FROM
            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
            LEFT JOIN EDW.TRAINING.VW_TRAINING tra ON tr.recordid = tra.trainingguid
        WHERE
            tr.eventtype IN (
                'Technology Training Multi-Port',
                'Technology Training Single-Port'
            )
            AND tr.courseid = 'TR100'
            AND tr.CERTIFICATIONDATE IS NOT NULL
            AND account.ClinicalRegion = 'Asia : Korea'
            AND (
                tr.role = 'Surgeon'
                OR tr.role IS NULL
            )
    ) b
GROUP BY
    b.year,
    b.quarter
UNION
ALL -- Bubble+ % (분기별, 외과의 기준 7+)
SELECT
    'Surgeons' AS "Category",
    s.year AS "Year",
    s.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Bubble+ %' AS "Type",
    TO_DECIMAL(
        AVG(
            CASE
                WHEN s.cases_per_qtr >= 7 THEN 1
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    'Bubble+ percentage' AS "Description"
FROM
    (
        /* 분기×외과의별 케이스 수 */
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            pr.surgeonguid,
            COUNT(DISTINCT pr.casenumber) AS cases_per_qtr
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.casestatus = 'Completed'
            AND pr.clinicalregion = 'Asia : Korea'
        GROUP BY
            1,
            2,
            3
    ) s
GROUP BY
    s.year,
    s.quarter
UNION
ALL -- Sustainable+ % (분기별, 외과의 기준 7+)
SELECT
    'Surgeons' AS "Category",
    s.year AS "Year",
    s.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    'Sustainable+ %' AS "Type",
    TO_DECIMAL(
        AVG(
            CASE
                WHEN s.cases_per_qtr >= 13 THEN 1
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    'Sustainable+ percentage' AS "Description"
FROM
    (
        /* 분기×외과의별 케이스 수 */
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            pr.surgeonguid,
            COUNT(DISTINCT pr.casenumber) AS cases_per_qtr
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.casestatus = 'Completed'
            AND pr.clinicalregion = 'Asia : Korea'
        GROUP BY
            1,
            2,
            3
    ) s
GROUP BY
    s.year,
    s.quarter
union
all
SELECT
    'Surgeon Efficiency' AS "Category",
    main."Year",
    main."Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '#of Procedure in before 8 am' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN main."Time_Flag" = 'before_8' THEN main."dV_Case"
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    '#of Procedure in before 8 am' AS "Description"
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS "Year",
            QUARTER(pr.localproceduredate) AS "Quarter",
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END AS "Time_Flag",
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            ) AS "Day_Flag",
            COUNT(DISTINCT pr.casenumber) AS "dV_Case"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND pr.accountid <> '21183'
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate),
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END,
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            )
    ) main
GROUP BY
    main."Year",
    main."Quarter"
union
all
SELECT
    'Surgeon Efficiency' AS "Category",
    main."Year",
    main."Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '#of Procedure in 8to17' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN main."Time_Flag" = '8to17' THEN main."dV_Case"
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    '#of Procedure in 8to17' AS "Description"
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS "Year",
            QUARTER(pr.localproceduredate) AS "Quarter",
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END AS "Time_Flag",
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            ) AS "Day_Flag",
            COUNT(DISTINCT pr.casenumber) AS "dV_Case"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND pr.accountid <> '21183'
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate),
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END,
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            )
    ) main
GROUP BY
    main."Year",
    main."Quarter"
union
all
SELECT
    'Surgeon Efficiency' AS "Category",
    main."Year",
    main."Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '#of Procedure in after 17 pm' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN main."Time_Flag" = 'after_17' THEN main."dV_Case"
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    '#of Procedure in before 17 pm' AS "Description"
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS "Year",
            QUARTER(pr.localproceduredate) AS "Quarter",
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END AS "Time_Flag",
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            ) AS "Day_Flag",
            COUNT(DISTINCT pr.casenumber) AS "dV_Case"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND pr.accountid <> '21183'
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate),
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END,
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            )
    ) main
GROUP BY
    main."Year",
    main."Quarter"
union
all
SELECT
    'Surgeon Efficiency' AS "Category",
    main."Year",
    main."Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '#of Procedure in weekday' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN main."Day_Flag" = 'Weekday' THEN main."dV_Case"
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    '#of Procedure in weekday' AS "Description"
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS "Year",
            QUARTER(pr.localproceduredate) AS "Quarter",
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END AS "Time_Flag",
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            ) AS "Day_Flag",
            COUNT(DISTINCT pr.casenumber) AS "dV_Case"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND pr.accountid <> '21183'
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate),
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END,
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            )
    ) main
GROUP BY
    main."Year",
    main."Quarter"
UNION
ALL
SELECT
    'Surgeon Efficiency' AS "Category",
    main."Year",
    main."Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '#of Procedure in weekend' AS "Type",
    TO_DECIMAL(
        SUM(
            CASE
                WHEN main."Day_Flag" = 'Weekend' THEN main."dV_Case"
                ELSE 0
            END
        ),
        15,
        2
    ) AS "Volume",
    '#of Procedure in weekend' AS "Description"
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS "Year",
            QUARTER(pr.localproceduredate) AS "Quarter",
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END AS "Time_Flag",
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            ) AS "Day_Flag",
            COUNT(DISTINCT pr.casenumber) AS "dV_Case"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND pr.accountid <> '21183'
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate),
            pr.accountguid,
            pr.accountid,
            CASE
                WHEN pr.starttimelocal IS NULL THEN 'unknown'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) < 8 THEN 'before_8'
                WHEN HOUR(
                    TO_TIMESTAMP_NTZ(
                        '1900-01-01 ' || pr.starttimelocal,
                        'YYYY-MM-DD HH12:MI AM'
                    )
                ) BETWEEN 8
                AND 17 THEN '8to17'
                ELSE 'after_17'
            END,
            COALESCE(
                CASE
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (1, 2, 3, 4, 5) THEN 'Weekday'
                    WHEN DAYOFWEEK(pr.localproceduredate) IN (0, 6) THEN 'Weekend'
                END,
                'Weekday'
            )
    ) main
GROUP BY
    main."Year",
    main."Quarter"