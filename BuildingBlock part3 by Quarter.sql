-- # of Surgeons 2+ cases per operating day
SELECT
    main2."Category",
    main2.year,
    main2.quarter,
    main2.ClinicalRegion,
    main2.type,
    COUNT(
        DISTINCT CASE
            WHEN main2."Volume" >= 2 THEN main2.surgeonid
        END
    ) AS volume,
    main2."Description"
FROM
    (
        SELECT
            'Surgeon Efficiency' AS "Category",
            main1."Year" AS year,
            main1."Quarter" AS quarter,
            main1.surgeonid,
            'Asia : Korea' AS ClinicalRegion,
            '# of Surgeons 2+ cases per operating day' AS type,
            TO_DECIMAL(
                AVG(
                    main1."#ofProc" / NULLIF(main1."#ofWorkingD", 0)
                ),
                15,
                2
            ) AS "Volume",
            '# of Surgeons 2+ cases per operating day' AS "Description"
        FROM
            (
                SELECT
                    pr.accountguid,
                    pr.accountid,
                    pr.accountname,
                    pr.surgeonguid,
                    pr.surgeonid,
                    YEAR(pr.localproceduredate) AS "Year",
                    QUARTER(pr.localproceduredate) AS "Quarter",
                    YEAR(pr.localproceduredate) || '-Q' || QUARTER(pr.localproceduredate) AS "Year-Quarter",
                    /* COUNT(DISTINCT pr.surgeonguid) AS "#ofActiveSurgeon", */
                    COUNT(DISTINCT pr.casenumber) AS "#ofProc",
                    COUNT(DISTINCT pr.localproceduredate) AS "#ofWorkingD"
                    /* cal."#ofWorkingD" AS "#ofWorkingD" */
                FROM
                    EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                WHERE
                    pr.clinicalregion = 'Asia : Korea'
                    AND pr.casestatus = 'Completed'
                GROUP BY
                    pr.accountguid,
                    pr.accountid,
                    pr.accountname,
                    pr.surgeonguid,
                    pr.surgeonid,
                    YEAR(pr.localproceduredate),
                    QUARTER(pr.localproceduredate),
                    YEAR(pr.localproceduredate) || '-Q' || QUARTER(pr.localproceduredate)
            ) main1
        GROUP BY
            main1."Year",
            main1."Quarter",
            main1.surgeonid
    ) main2
GROUP BY
    main2."Category",
    main2.year,
    main2.quarter,
    main2.ClinicalRegion,
    main2.type,
    main2."Description"
UNION
ALL
SELECT
    main2."Category",
    main2.year,
    main2.quarter,
    main2.ClinicalRegion,
    main2.type,
    COUNT(
        DISTINCT CASE
            WHEN main2."Volume" >= 3 THEN main2.surgeonid
        END
    ) AS volume,
    main2."Description"
FROM
    (
        SELECT
            'Surgeon Efficiency' AS "Category",
            main1."Year" AS year,
            main1."Quarter" AS quarter,
            main1.surgeonid,
            'Asia : Korea' AS ClinicalRegion,
            '# of Surgeons 3+ cases per operating day' AS type,
            TO_DECIMAL(
                AVG(
                    main1."#ofProc" / NULLIF(main1."#ofWorkingD", 0)
                ),
                15,
                2
            ) AS "Volume",
            '# of Surgeons 3+ cases per operating day' AS "Description"
        FROM
            (
                SELECT
                    pr.accountguid,
                    pr.accountid,
                    pr.accountname,
                    pr.surgeonguid,
                    pr.surgeonid,
                    YEAR(pr.localproceduredate) AS "Year",
                    QUARTER(pr.localproceduredate) AS "Quarter",
                    YEAR(pr.localproceduredate) || '-Q' || QUARTER(pr.localproceduredate) AS "Year-Quarter",
                    /* COUNT(DISTINCT pr.surgeonguid) AS "#ofActiveSurgeon", */
                    COUNT(DISTINCT pr.casenumber) AS "#ofProc",
                    COUNT(DISTINCT pr.localproceduredate) AS "#ofWorkingD"
                    /* cal."#ofWorkingD" AS "#ofWorkingD" */
                FROM
                    EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                WHERE
                    pr.clinicalregion = 'Asia : Korea'
                    AND pr.casestatus = 'Completed'
                GROUP BY
                    pr.accountguid,
                    pr.accountid,
                    pr.accountname,
                    pr.surgeonguid,
                    pr.surgeonid,
                    YEAR(pr.localproceduredate),
                    QUARTER(pr.localproceduredate),
                    YEAR(pr.localproceduredate) || '-Q' || QUARTER(pr.localproceduredate)
            ) main1
        GROUP BY
            main1."Year",
            main1."Quarter",
            main1.surgeonid
    ) main2
GROUP BY
    main2."Category",
    main2.year,
    main2.quarter,
    main2.ClinicalRegion,
    main2.type,
    main2."Description"
UNION
ALL
/* % of SSM w/ 2 Incremental Sustainable+ */
SELECT
    'Surgeon Evolution' AS "Category",
    t.year AS "Year",
    t.quarter AS "Quarter",
    t.ClinicalRegion AS "ClinicalRegion",
    '% of SSM w/ 2 Incremental Sustainable+' AS "Type",
    CASE
        WHEN t.ssm_cnt = 0 THEN TO_DECIMAL(h.ssm_hit_cnt, 15, 1)
        ELSE TO_DECIMAL(h.ssm_hit_cnt :: FLOAT / t.ssm_cnt, 15, 2)
    END AS "Volume",
    '2 Incremental # Sustainable+ / # SSM' AS "Description"
FROM
    (
        /* (A) 분기×지역 전체 SSM 모수 */
        SELECT
            q.year,
            q.quarter,
            q.ClinicalRegion,
            COUNT(DISTINCT q.histssm) AS ssm_cnt
        FROM
            (
                /* 분기별 accountguid ↔ SSM 매핑 */
                SELECT
                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                    acc.ClinicalRegion,
                    ha.accountguid,
                    ha.histssm
                FROM
                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                WHERE
                    acc.ClinicalRegion = 'Asia : Korea'
                    AND ha.histssm IS NOT NULL
                    AND LOWER(ha.histssm) NOT LIKE '%open%'
            ) q
        GROUP BY
            q.year,
            q.quarter,
            q.ClinicalRegion
    ) t
    LEFT JOIN (
        /* (B) 증분합계 ≥ 2 인 SSM 수 (분기×지역) */
        SELECT
            z.year,
            z.quarter,
            z.ClinicalRegion,
            COUNT(DISTINCT z.histssm) AS ssm_hit_cnt
        FROM
            (
                /* 각 분기, SSM 포트폴리오(담당 계정들)의 Sustainable+ 증분 합계 계산 → ≥2 필터 */
                SELECT
                    s.year,
                    s.quarter,
                    s.ClinicalRegion,
                    s.histssm
                FROM
                    (
                        SELECT
                            m.year,
                            m.quarter,
                            m.ClinicalRegion,
                            m.histssm,
                            SUM(m.delta) AS sum_delta
                        FROM
                            (
                                /* 계정×분기의 Sustainable+ 합계와 전년동기 대비 증분(delta) */
                                SELECT
                                    ba.year,
                                    ba.quarter,
                                    ba.ClinicalRegion,
                                    qm.histssm,
                                    /* 계정별 Sustainable+ 합계의 YoY 증분 */
                                    (
                                        ba.bubble_sum - LAG(ba.bubble_sum, 4) OVER (
                                            PARTITION BY ba.accountguid
                                            ORDER BY
                                                (ba.year * 10 + ba.quarter)
                                        )
                                    ) AS delta
                                FROM
                                    (
                                        /* 계정×분기: Surgeon별 Sustainable+(13+) 플래그 합계 */
                                        SELECT
                                            bs.year,
                                            bs.quarter,
                                            bs.ClinicalRegion,
                                            bs.accountguid,
                                            SUM(bs.bubble_flag) AS bubble_sum
                                        FROM
                                            (
                                                /* Surgeon×Account×Quarter: 지정 Subject의 건수가 13+ 이면 1 */
                                                SELECT
                                                    YEAR(p.proceduredatelocal) AS year,
                                                    QUARTER(p.proceduredatelocal) AS quarter,
                                                    a.ClinicalRegion AS ClinicalRegion,
                                                    p.accountguid,
                                                    p.surgeonguid,
                                                    CASE
                                                        WHEN COUNT(
                                                            CASE
                                                                WHEN CONCAT(p.businesscategoryname, p.subject) IN (
                                                                    'General SurgeryBreast Surgery',
                                                                    'ThoracicEsophagectomy',
                                                                    'ThoracicForegut',
                                                                    'ThoracicLung Resection',
                                                                    'ThoracicOther Thoracic',
                                                                    'ThoracicRectal Resection',
                                                                    'ThoracicWedge Resection'
                                                                ) THEN p.recordid
                                                            END
                                                        ) >= 13 THEN 1
                                                        ELSE 0
                                                    END AS bubble_flag
                                                FROM
                                                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
                                                    /* contact join 불필요하여 제거 */
                                                WHERE
                                                    p.status = 'Completed'
                                                    AND a.recordtype = 'Hospital'
                                                    AND a.ClinicalRegion = 'Asia : Korea'
                                                GROUP BY
                                                    YEAR(p.proceduredatelocal),
                                                    QUARTER(p.proceduredatelocal),
                                                    a.ClinicalRegion,
                                                    p.accountguid,
                                                    p.surgeonguid
                                            ) bs
                                        GROUP BY
                                            bs.year,
                                            bs.quarter,
                                            bs.ClinicalRegion,
                                            bs.accountguid
                                    ) ba
                                    /* 같은 분기/계정의 담당 SSM 매핑 */
                                    JOIN (
                                        SELECT
                                            TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                                            TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                                            acc.ClinicalRegion,
                                            ha.accountguid,
                                            ha.histssm
                                        FROM
                                            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                                        WHERE
                                            acc.ClinicalRegion = 'Asia : Korea'
                                            AND ha.histssm IS NOT NULL
                                            AND LOWER(ha.histssm) NOT LIKE '%open%'
                                    ) qm ON qm.year = ba.year
                                    AND qm.quarter = ba.quarter
                                    AND qm.ClinicalRegion = ba.ClinicalRegion
                                    AND qm.accountguid = ba.accountguid
                            ) m
                        GROUP BY
                            m.year,
                            m.quarter,
                            m.ClinicalRegion,
                            m.histssm
                        HAVING
                            SUM(m.delta) >= 2
                    ) s
            ) z
        GROUP BY
            z.year,
            z.quarter,
            z.ClinicalRegion
    ) h ON h.year = t.year
    AND h.quarter = t.quarter
    AND h.ClinicalRegion = t.ClinicalRegion
UNION
ALL -- % of New Installs < 3 cases per week
SELECT
    'New Install Productivity & Mature System Utilization' AS "Category",
    m.year AS "Year",
    m.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '% of New Installs < 3 cases per week' AS "Type",
    TO_DECIMAL(
        COUNT(
            DISTINCT CASE
                WHEN m.volume < 3 THEN m."SystemName"
            END
        ) / NULLIF(COUNT(DISTINCT m."SystemName"), 0),
        15,
        2
    ) AS "Volume",
    '% of New Installs < 3 cases per week' AS "Description"
FROM
    (
        /* --------- (A) Non-zero procedure in the given period --------- */
        SELECT
            DISTINCT a.year,
            a.quarter,
            a.accountguid,
            a.accountid,
            a.accountname,
            a.systemguid,
            a."SystemName",
            a."SystemModel",
            a.installdate,
            /* 주당 케이스 수 */
            a."#ofProc" / a."#ofwk" AS volume
        FROM
            (
                SELECT
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    COUNT(DISTINCT pr.casenumber) AS "#ofProc",
                    /* 0 또는 음수 보호: 최소 0.1주로 처리 */
                    CASE
                        WHEN ib."#ofwk" <= 0 THEN 0.1
                        ELSE ib."#ofwk"
                    END AS "#ofwk"
                FROM
                    (
                        /* 설치 후 1년 내 해당 분기별 시스템-분기 집합 */
                        SELECT
                            dim.year,
                            dim.quarter,
                            ib.accountguid,
                            ib.accountid,
                            ib.accountname,
                            ib.systemguid,
                            ib.name AS "SystemName",
                            ib.model AS "SystemModel",
                            ib.installdate,
                            /* 설치분기라면 해당 분기에서 설치일~분기말까지의 (일수+1)/7, 그 외는 13주 가정 */
                            CASE
                                WHEN dim.year = YEAR(ib.installdate)
                                AND dim.quarter = QUARTER(ib.installdate) THEN (DATEDIFF('day', ib.installdate, dim.lb_day) + 1) / 7
                                ELSE 13
                            END AS "#ofwk"
                        FROM
                            (
                                SELECT
                                    DISTINCT YEAR(cal_day) AS year,
                                    QUARTER(cal_day) AS quarter,
                                    sf_dc_week AS sfdcweek,
                                    cal_quarter_lb_day AS lb_day,
                                    cal_day AS calendar_dt
                                FROM
                                    EDW.MASTER_DATA.VW_ISICALENDAR
                                WHERE
                                    YEAR(cal_day) BETWEEN 2000
                                    AND 2029
                                    AND country = 'KR'
                            ) dim
                            LEFT JOIN EDW.MASTER.VW_INSTALLBASE ib ON DATEADD('year', 1, ib.installdate) >= dim.calendar_dt
                            AND ib.installdate <= dim.calendar_dt
                        WHERE
                            ib.type = 'System'
                            AND ib.ELIGIBILITY = 'Human Use - OR'
                            /* 설치 1년 내 제거된 시스템 제외: NOT EXISTS 로 NULL 안전 */
                            AND NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    EDW.MASTER.VW_INSTALLBASE ib2
                                WHERE
                                    ib2.accountguid = ib.accountguid
                                    AND ib2.systemguid = ib.systemguid
                                    AND ib2.removedate <= DATEADD('day', 365, ib2.installdate)
                            )
                            AND ib.accountguid IN (
                                SELECT
                                    ac.ACCOUNT_GUID
                                FROM
                                    EDW.MASTER_DATA.VW_ACCOUNT ac
                                WHERE
                                    ac.clinical_region = 'Asia : Korea'
                            )
                    ) ib
                    LEFT JOIN (
                        /* 해당 분기의 수술 케이스 */
                        SELECT
                            pr.systemname,
                            pr.systemmodelname,
                            pr.localproceduredate,
                            pr.casenumber
                        FROM
                            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                        WHERE
                            pr.clinicalregion = 'Asia : Korea'
                            AND pr.casestatus = 'Completed'
                    ) pr ON ib."SystemName" = pr.systemname
                    AND CONCAT(
                        YEAR(pr.localproceduredate),
                        '-Q',
                        QUARTER(pr.localproceduredate)
                    ) = CONCAT(ib.year, '-Q', ib.quarter)
                GROUP BY
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    ib."#ofwk"
            ) a
        WHERE
            a."#ofProc" <> 0
        UNION
        ALL
        /* --------- (B) Zero procedure in the given period --------- */
        SELECT
            DISTINCT b.year,
            b.quarter,
            b.accountguid,
            b.accountid,
            b.accountname,
            b.systemguid,
            b."SystemName",
            b."SystemModel",
            b.installdate,
            0 AS volume
        FROM
            (
                /* 위 (A)와 동일 소스, 단 0프로시저 필터 */
                SELECT
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    COUNT(DISTINCT pr.casenumber) AS "#ofProc",
                    CASE
                        WHEN ib."#ofwk" <= 0 THEN 0.1
                        ELSE ib."#ofwk"
                    END AS "#ofwk"
                FROM
                    (
                        SELECT
                            dim.year,
                            dim.quarter,
                            ib.accountguid,
                            ib.accountid,
                            ib.accountname,
                            ib.systemguid,
                            ib.name AS "SystemName",
                            ib.model AS "SystemModel",
                            ib.installdate,
                            CASE
                                WHEN dim.year = YEAR(ib.installdate)
                                AND dim.quarter = QUARTER(ib.installdate) THEN (DATEDIFF('day', ib.installdate, dim.lb_day) + 1) / 7
                                ELSE 13
                            END AS "#ofwk"
                        FROM
                            (
                                SELECT
                                    DISTINCT YEAR(cal_day) AS year,
                                    QUARTER(cal_day) AS quarter,
                                    sf_dc_week AS sfdcweek,
                                    cal_quarter_lb_day AS lb_day,
                                    cal_day AS calendar_dt
                                FROM
                                    EDW.MASTER_DATA.VW_ISICALENDAR
                                WHERE
                                    YEAR(cal_day) BETWEEN 2000
                                    AND 2029
                                    AND country = 'KR'
                            ) dim
                            LEFT JOIN EDW.MASTER.VW_INSTALLBASE ib ON DATEADD('year', 1, ib.installdate) >= dim.calendar_dt
                            AND ib.installdate <= dim.calendar_dt
                        WHERE
                            ib.type = 'System'
                            AND ib.ELIGIBILITY = 'Human Use - OR'
                            AND NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    EDW.MASTER.VW_INSTALLBASE ib2
                                WHERE
                                    ib2.accountguid = ib.accountguid
                                    AND ib2.systemguid = ib.systemguid
                                    AND ib2.removedate <= DATEADD('day', 365, ib2.installdate)
                            )
                            AND ib.accountguid IN (
                                SELECT
                                    ac.ACCOUNT_GUID
                                FROM
                                    EDW.MASTER_DATA.VW_ACCOUNT ac
                                WHERE
                                    ac.clinical_region = 'Asia : Korea'
                            )
                    ) ib
                    LEFT JOIN (
                        SELECT
                            pr.systemname,
                            pr.systemmodelname,
                            pr.localproceduredate,
                            pr.casenumber
                        FROM
                            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                        WHERE
                            pr.clinicalregion = 'Asia : Korea'
                            AND pr.casestatus = 'Completed'
                    ) pr ON ib."SystemName" = pr.systemname
                    AND CONCAT(
                        YEAR(pr.localproceduredate),
                        '-Q',
                        QUARTER(pr.localproceduredate)
                    ) = CONCAT(ib.year, '-Q', ib.quarter)
                GROUP BY
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    ib."#ofwk"
            ) b
        WHERE
            b."#ofProc" = 0
    ) m
GROUP BY
    m.year,
    m.quarter
UNION
ALL -- % of New Installs < 1 cases per week
SELECT
    'New Install Productivity & Mature System Utilization' AS "Category",
    m.year AS "Year",
    m.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '% of New Installs < 1 cases per week' AS "Type",
    TO_DECIMAL(
        COUNT(
            DISTINCT CASE
                WHEN m.volume < 1 THEN m."SystemName"
            END
        ) / NULLIF(COUNT(DISTINCT m."SystemName"), 0),
        15,
        2
    ) AS "Volume",
    '% of New Installs < 1 cases per week' AS "Description"
FROM
    (
        /* --------- (A) Non-zero procedure in the given period --------- */
        SELECT
            DISTINCT a.year,
            a.quarter,
            a.accountguid,
            a.accountid,
            a.accountname,
            a.systemguid,
            a."SystemName",
            a."SystemModel",
            a.installdate,
            /* 주당 케이스 수 */
            a."#ofProc" / a."#ofwk" AS volume
        FROM
            (
                SELECT
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    COUNT(DISTINCT pr.casenumber) AS "#ofProc",
                    /* 0 또는 음수 보호: 최소 0.1주로 처리 */
                    CASE
                        WHEN ib."#ofwk" <= 0 THEN 0.1
                        ELSE ib."#ofwk"
                    END AS "#ofwk"
                FROM
                    (
                        /* 설치 후 1년 내 해당 분기별 시스템-분기 집합 */
                        SELECT
                            dim.year,
                            dim.quarter,
                            ib.accountguid,
                            ib.accountid,
                            ib.accountname,
                            ib.systemguid,
                            ib.name AS "SystemName",
                            ib.model AS "SystemModel",
                            ib.installdate,
                            /* 설치분기라면 해당 분기에서 설치일~분기말까지의 (일수+1)/7, 그 외는 13주 가정 */
                            CASE
                                WHEN dim.year = YEAR(ib.installdate)
                                AND dim.quarter = QUARTER(ib.installdate) THEN (DATEDIFF('day', ib.installdate, dim.lb_day) + 1) / 7
                                ELSE 13
                            END AS "#ofwk"
                        FROM
                            (
                                SELECT
                                    DISTINCT YEAR(cal_day) AS year,
                                    QUARTER(cal_day) AS quarter,
                                    sf_dc_week AS sfdcweek,
                                    cal_quarter_lb_day AS lb_day,
                                    cal_day AS calendar_dt
                                FROM
                                    EDW.MASTER_DATA.VW_ISICALENDAR
                                WHERE
                                    YEAR(cal_day) BETWEEN 2000
                                    AND 2029
                                    AND country = 'KR'
                            ) dim
                            LEFT JOIN EDW.MASTER.VW_INSTALLBASE ib ON DATEADD('year', 1, ib.installdate) >= dim.calendar_dt
                            AND ib.installdate <= dim.calendar_dt
                        WHERE
                            ib.type = 'System'
                            AND ib.ELIGIBILITY = 'Human Use - OR'
                            /* 설치 1년 내 제거된 시스템 제외: NOT EXISTS 로 NULL 안전 */
                            AND NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    EDW.MASTER.VW_INSTALLBASE ib2
                                WHERE
                                    ib2.accountguid = ib.accountguid
                                    AND ib2.systemguid = ib.systemguid
                                    AND ib2.removedate <= DATEADD('day', 365, ib2.installdate)
                            )
                            AND ib.accountguid IN (
                                SELECT
                                    ac.ACCOUNT_GUID
                                FROM
                                    EDW.MASTER_DATA.VW_ACCOUNT ac
                                WHERE
                                    ac.clinical_region = 'Asia : Korea'
                            )
                    ) ib
                    LEFT JOIN (
                        /* 해당 분기의 수술 케이스 */
                        SELECT
                            pr.systemname,
                            pr.systemmodelname,
                            pr.localproceduredate,
                            pr.casenumber
                        FROM
                            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                        WHERE
                            pr.clinicalregion = 'Asia : Korea'
                            AND pr.casestatus = 'Completed'
                    ) pr ON ib."SystemName" = pr.systemname
                    AND CONCAT(
                        YEAR(pr.localproceduredate),
                        '-Q',
                        QUARTER(pr.localproceduredate)
                    ) = CONCAT(ib.year, '-Q', ib.quarter)
                GROUP BY
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    ib."#ofwk"
            ) a
        WHERE
            a."#ofProc" <> 0
        UNION
        ALL
        /* --------- (B) Zero procedure in the given period --------- */
        SELECT
            DISTINCT b.year,
            b.quarter,
            b.accountguid,
            b.accountid,
            b.accountname,
            b.systemguid,
            b."SystemName",
            b."SystemModel",
            b.installdate,
            0 AS volume
        FROM
            (
                /* 위 (A)와 동일 소스, 단 0프로시저 필터 */
                SELECT
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    COUNT(DISTINCT pr.casenumber) AS "#ofProc",
                    CASE
                        WHEN ib."#ofwk" <= 0 THEN 0.1
                        ELSE ib."#ofwk"
                    END AS "#ofwk"
                FROM
                    (
                        SELECT
                            dim.year,
                            dim.quarter,
                            ib.accountguid,
                            ib.accountid,
                            ib.accountname,
                            ib.systemguid,
                            ib.name AS "SystemName",
                            ib.model AS "SystemModel",
                            ib.installdate,
                            CASE
                                WHEN dim.year = YEAR(ib.installdate)
                                AND dim.quarter = QUARTER(ib.installdate) THEN (DATEDIFF('day', ib.installdate, dim.lb_day) + 1) / 7
                                ELSE 13
                            END AS "#ofwk"
                        FROM
                            (
                                SELECT
                                    DISTINCT YEAR(cal_day) AS year,
                                    QUARTER(cal_day) AS quarter,
                                    sf_dc_week AS sfdcweek,
                                    cal_quarter_lb_day AS lb_day,
                                    cal_day AS calendar_dt
                                FROM
                                    EDW.MASTER_DATA.VW_ISICALENDAR
                                WHERE
                                    YEAR(cal_day) BETWEEN 2000
                                    AND 2029
                                    AND country = 'KR'
                            ) dim
                            LEFT JOIN EDW.MASTER.VW_INSTALLBASE ib ON DATEADD('year', 1, ib.installdate) >= dim.calendar_dt
                            AND ib.installdate <= dim.calendar_dt
                        WHERE
                            ib.type = 'System'
                            AND ib.ELIGIBILITY = 'Human Use - OR'
                            AND NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    EDW.MASTER.VW_INSTALLBASE ib2
                                WHERE
                                    ib2.accountguid = ib.accountguid
                                    AND ib2.systemguid = ib.systemguid
                                    AND ib2.removedate <= DATEADD('day', 365, ib2.installdate)
                            )
                            AND ib.accountguid IN (
                                SELECT
                                    ac.ACCOUNT_GUID
                                FROM
                                    EDW.MASTER_DATA.VW_ACCOUNT ac
                                WHERE
                                    ac.clinical_region = 'Asia : Korea'
                            )
                    ) ib
                    LEFT JOIN (
                        SELECT
                            pr.systemname,
                            pr.systemmodelname,
                            pr.localproceduredate,
                            pr.casenumber
                        FROM
                            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                        WHERE
                            pr.clinicalregion = 'Asia : Korea'
                            AND pr.casestatus = 'Completed'
                    ) pr ON ib."SystemName" = pr.systemname
                    AND CONCAT(
                        YEAR(pr.localproceduredate),
                        '-Q',
                        QUARTER(pr.localproceduredate)
                    ) = CONCAT(ib.year, '-Q', ib.quarter)
                GROUP BY
                    ib.year,
                    ib.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib."SystemName",
                    ib."SystemModel",
                    ib.installdate,
                    ib."#ofwk"
            ) b
        WHERE
            b."#ofProc" = 0
    ) m
GROUP BY
    m.year,
    m.quarter
UNION
ALL -- % of Mature Xi < 1 cases per week
SELECT
    'New Install Productivity & Mature System Utilization' AS "Category",
    m.year AS "Year",
    m.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '% of Mature Xi < 1 cases per week' AS "Type",
    TO_DECIMAL(
        COUNT(
            DISTINCT CASE
                WHEN m.volume < 1 THEN m."SystemName"
            END
        ) / NULLIF(COUNT(DISTINCT m."SystemName"), 0),
        15,
        2
    ) AS "Volume",
    '% of Mature Xi < 1 cases per week' AS "Description"
FROM
    (
        /* 성숙: 설치 1년 경과(분기말 기준) Xi 시스템의 분기 평균 주간 케이스(= 분기 케이스/13) */
        SELECT
            ib_q.year,
            ib_q.quarter,
            ib_q.accountguid,
            ib_q.accountid,
            ib_q.accountname,
            ib_q.systemguid,
            ib_q."SystemName",
            ib_q."SystemModel",
            ib_q.installdate,
            /* 분기 평균 주간 케이스 */
            (COUNT(DISTINCT pr.casenumber) / 13.0) AS volume
        FROM
            (
                /* 분기(연-분기, 분기말 1행) × Xi 설치기준(분기말에 설치 후 1년 이상 경과) */
                SELECT
                    dq.year,
                    dq.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib.name AS "SystemName",
                    ib.model AS "SystemModel",
                    ib.installdate
                FROM
                    (
                        /* 분기말(quarter_end)만 남긴 캘린더: 연×분기 1행 */
                        SELECT
                            YEAR(calendar_dt) AS year,
                            QUARTER(calendar_dt) AS quarter,
                            MAX(calendar_dt) AS quarter_end
                        FROM
                            EDW.MASTER.VW_DIMDATE
                        WHERE
                            YEAR(calendar_dt) BETWEEN 2000
                            AND 2029
                        GROUP BY
                            YEAR(calendar_dt),
                            QUARTER(calendar_dt)
                    ) dq
                    LEFT JOIN EDW.MASTER.VW_INSTALLBASE ib ON ib.type = 'System'
                    AND ib.model = 'da Vinci Xi'
                    AND ib.ELIGIBILITY = 'Human Use - OR'
                    /* 분기말 기준으로 설치 후 1년 경과(= Mature) */
                    AND DATEADD('year', 1, ib.installdate) <= dq.quarter_end
                    /* 설치 1년 내 제거된 시스템 제외 (NULL 안전) */
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            EDW.MASTER.VW_INSTALLBASE ib2
                        WHERE
                            ib2.systemguid = ib.systemguid
                            AND ib2.accountguid = ib.accountguid
                            AND ib2.removedate <= DATEADD('day', 365, ib2.installdate)
                    )
                WHERE
                    ib.accountguid IN (
                        SELECT
                            ac.ACCOUNT_GUID
                        FROM
                            EDW.MASTER_DATA.VW_ACCOUNT ac
                        WHERE
                            ac.clinical_region = 'Asia : Korea'
                    )
            ) ib_q
            LEFT JOIN (
                /* 해당 분기의 Xi 케이스 */
                SELECT
                    pr.systemname,
                    pr.systemmodelname,
                    pr.localproceduredate,
                    pr.casenumber
                FROM
                    EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                WHERE
                    pr.clinicalregion = 'Asia : Korea'
                    AND pr.casestatus = 'Completed'
            ) pr ON ib_q."SystemName" = pr.systemname
            AND YEAR(pr.localproceduredate) = ib_q.year
            AND QUARTER(pr.localproceduredate) = ib_q.quarter
        GROUP BY
            ib_q.year,
            ib_q.quarter,
            ib_q.accountguid,
            ib_q.accountid,
            ib_q.accountname,
            ib_q.systemguid,
            ib_q."SystemName",
            ib_q."SystemModel",
            ib_q.installdate
    ) m
GROUP BY
    m.year,
    m.quarter
UNION
ALL -- % of Mature Xi < 3 cases per week
SELECT
    'New Install Productivity & Mature System Utilization' AS "Category",
    m.year AS "Year",
    m.quarter AS "Quarter",
    'Asia : Korea' AS "ClinicalRegion",
    '% of Mature Xi < 3 cases per week' AS "Type",
    TO_DECIMAL(
        COUNT(
            DISTINCT CASE
                WHEN m.volume < 3 THEN m."SystemName"
            END
        ) / NULLIF(COUNT(DISTINCT m."SystemName"), 0),
        15,
        2
    ) AS "Volume",
    '% of Mature Xi < 3 cases per week' AS "Description"
FROM
    (
        /* 성숙: 설치 1년 경과(분기말 기준) Xi 시스템의 분기 평균 주간 케이스(= 분기 케이스/13) */
        SELECT
            ib_q.year,
            ib_q.quarter,
            ib_q.accountguid,
            ib_q.accountid,
            ib_q.accountname,
            ib_q.systemguid,
            ib_q."SystemName",
            ib_q."SystemModel",
            ib_q.installdate,
            /* 분기 평균 주간 케이스 */
            (COUNT(DISTINCT pr.casenumber) / 13.0) AS volume
        FROM
            (
                /* 분기(연-분기, 분기말 1행) × Xi 설치기준(분기말에 설치 후 1년 이상 경과) */
                SELECT
                    dq.year,
                    dq.quarter,
                    ib.accountguid,
                    ib.accountid,
                    ib.accountname,
                    ib.systemguid,
                    ib.name AS "SystemName",
                    ib.model AS "SystemModel",
                    ib.installdate
                FROM
                    (
                        /* 분기말(quarter_end)만 남긴 캘린더: 연×분기 1행 */
                        SELECT
                            YEAR(calendar_dt) AS year,
                            QUARTER(calendar_dt) AS quarter,
                            MAX(calendar_dt) AS quarter_end
                        FROM
                            EDW.MASTER.VW_DIMDATE
                        WHERE
                            YEAR(calendar_dt) BETWEEN 2000
                            AND 2029
                        GROUP BY
                            YEAR(calendar_dt),
                            QUARTER(calendar_dt)
                    ) dq
                    LEFT JOIN EDW.MASTER.VW_INSTALLBASE ib ON ib.type = 'System'
                    AND ib.model = 'da Vinci Xi'
                    AND ib.ELIGIBILITY = 'Human Use - OR'
                    /* 분기말 기준으로 설치 후 1년 경과(= Mature) */
                    AND DATEADD('year', 1, ib.installdate) <= dq.quarter_end
                    /* 설치 1년 내 제거된 시스템 제외 (NULL 안전) */
                    AND NOT EXISTS (
                        SELECT
                            1
                        FROM
                            EDW.MASTER.VW_INSTALLBASE ib2
                        WHERE
                            ib2.systemguid = ib.systemguid
                            AND ib2.accountguid = ib.accountguid
                            AND ib2.removedate <= DATEADD('day', 365, ib2.installdate)
                    )
                WHERE
                    ib.accountguid IN (
                        SELECT
                            ac.ACCOUNT_GUID
                        FROM
                            EDW.MASTER_DATA.VW_ACCOUNT ac
                        WHERE
                            ac.clinical_region = 'Asia : Korea'
                    )
            ) ib_q
            LEFT JOIN (
                /* 해당 분기의 Xi 케이스 */
                SELECT
                    pr.systemname,
                    pr.systemmodelname,
                    pr.localproceduredate,
                    pr.casenumber
                FROM
                    EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
                WHERE
                    pr.clinicalregion = 'Asia : Korea'
                    AND pr.casestatus = 'Completed'
            ) pr ON ib_q."SystemName" = pr.systemname
            AND YEAR(pr.localproceduredate) = ib_q.year
            AND QUARTER(pr.localproceduredate) = ib_q.quarter
        GROUP BY
            ib_q.year,
            ib_q.quarter,
            ib_q.accountguid,
            ib_q.accountid,
            ib_q.accountname,
            ib_q.systemguid,
            ib_q."SystemName",
            ib_q."SystemModel",
            ib_q.installdate
    ) m
GROUP BY
    m.year,
    m.quarter
UNION
ALL
SELECT
    'Advanced Tech Penetration' AS "Category",
    main.year,
    main.quarter,
    'Asia : Korea' AS Clinicalregion,
    'Stapler Penetration' AS "Type",
    to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
    'Lung Resection' AS description
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            / / pr.surgeonid,
            COUNT(
                DISTINCT CASE
                    WHEN pr.staplerused = 'Y'
                    AND pr.proceduresubject = 'Lung Resection' THEN pr.casenumber
                END
            ) AS "UsedCase",
            COUNT(
                DISTINCT CASE
                    WHEN pr.proceduresubject = 'Lung Resection' THEN pr.casenumber
                END
            ) AS "TotalCase"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND YEAR(pr.localproceduredate) >= 2020
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate) / / pr.surgeonid
    ) main
GROUP BY
    main.year,
    main.quarter / / to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
UNION
ALL
SELECT
    'Advanced Tech Penetration' AS "Category",
    main.year,
    main.quarter,
    'Asia : Korea' AS Clinicalregion,
    'Stapler Penetration' AS "Type",
    to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
    'Rectal Resection' AS description
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            / / pr.surgeonid,
            COUNT(
                DISTINCT CASE
                    WHEN pr.staplerused = 'Y'
                    AND pr.proceduresubject = 'Rectal Resection' THEN pr.casenumber
                END
            ) AS "UsedCase",
            COUNT(
                DISTINCT CASE
                    WHEN pr.proceduresubject = 'Rectal Resection' THEN pr.casenumber
                END
            ) AS "TotalCase"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND YEAR(pr.localproceduredate) >= 2020
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate) / / pr.surgeonid
    ) main
GROUP BY
    main.year,
    main.quarter / / to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
UNION
ALL
SELECT
    'Advanced Tech Penetration' AS "Category",
    main.year,
    main.quarter,
    'Asia : Korea' AS Clinicalregion,
    'Stapler Penetration' AS "Type",
    to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
    'Gastrectomy' AS description
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            / / pr.surgeonid,
            COUNT(
                DISTINCT CASE
                    WHEN pr.staplerused = 'Y'
                    AND pr.proceduresubject = 'Gastrectomy' THEN pr.casenumber
                END
            ) AS "UsedCase",
            COUNT(
                DISTINCT CASE
                    WHEN pr.proceduresubject = 'Gastrectomy' THEN pr.casenumber
                END
            ) AS "TotalCase"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND YEAR(pr.localproceduredate) >= 2020
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate) / / pr.surgeonid
    ) main
GROUP BY
    main.year,
    main.quarter / / to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
UNION
ALL
SELECT
    'Advanced Tech Penetration' AS "Category",
    main.year,
    main.quarter,
    'Asia : Korea' AS Clinicalregion,
    'VS/SS Penetration' AS "Type",
    to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
    'Hysterectomy - Malignant' AS description
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            / / pr.surgeonid,
            COUNT(
                DISTINCT CASE
                    WHEN (
                        pr.vesselsealerused = 'Y'
                        OR pr.synchrosealused = 'Y'
                    )
                    AND pr.proceduresubject = 'Hysterectomy - Malignant' THEN pr.casenumber
                END
            ) AS "UsedCase",
            COUNT(
                DISTINCT CASE
                    WHEN pr.proceduresubject = 'Hysterectomy - Malignant' THEN pr.casenumber
                END
            ) AS "TotalCase"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND YEAR(pr.localproceduredate) >= 2020
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate) / / pr.surgeonid
    ) main
GROUP BY
    main.year,
    main.quarter / / to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
UNION
ALL
SELECT
    'Advanced Tech Penetration' AS "Category",
    main.year,
    main.quarter,
    'Asia : Korea' AS Clinicalregion,
    'VS/SS Penetration' AS "Type",
    to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
    'Hysterectomy - Benign' AS description
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            / / pr.surgeonid,
            COUNT(
                DISTINCT CASE
                    WHEN (
                        pr.vesselsealerused = 'Y'
                        OR pr.synchrosealused = 'Y'
                    )
                    AND pr.proceduresubject = 'Hysterectomy - Benign' THEN pr.casenumber
                END
            ) AS "UsedCase",
            COUNT(
                DISTINCT CASE
                    WHEN pr.proceduresubject = 'Hysterectomy - Benign' THEN pr.casenumber
                END
            ) AS "TotalCase"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND YEAR(pr.localproceduredate) >= 2020
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate) / / pr.surgeonid
    ) main
GROUP BY
    main.year,
    main.quarter / / to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
UNION
ALL
SELECT
    'Advanced Tech Penetration' AS "Category",
    main.year,
    main.quarter,
    'Asia : Korea' AS Clinicalregion,
    'VS/SS Penetration' AS "Type",
    to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,
    'Rectal Resection' AS description
FROM
    (
        SELECT
            YEAR(pr.localproceduredate) AS year,
            QUARTER(pr.localproceduredate) AS quarter,
            / / pr.surgeonid,
            COUNT(
                DISTINCT CASE
                    WHEN (
                        pr.vesselsealerused = 'Y'
                        OR pr.synchrosealused = 'Y'
                    )
                    AND pr.proceduresubject = 'Rectal Resection' THEN pr.casenumber
                END
            ) AS "UsedCase",
            COUNT(
                DISTINCT CASE
                    WHEN pr.proceduresubject = 'Rectal Resection' THEN pr.casenumber
                END
            ) AS "TotalCase"
        FROM
            EDW.PROCEDURES.VW_PROCEDURESUMMARY pr
        WHERE
            pr.clinicalregion = 'Asia : Korea'
            AND pr.casestatus = 'Completed'
            AND YEAR(pr.localproceduredate) >= 2020
        GROUP BY
            YEAR(pr.localproceduredate),
            QUARTER(pr.localproceduredate) / / pr.surgeonid
    ) main
GROUP BY
    main.year,
    main.quarter / / to_decimal(
        SUM(main."UsedCase") / SUM(main."TotalCase"),
        15,
        2
    ) AS volume,