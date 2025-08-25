/* % of CSRs w/ ≥1 Incremental Surgeon (≥7 / ≥13) — latest-date account mapping */
SELECT
    'Surgeon Evolution' AS "Category",
    t.year AS "Year",
    t.quarter AS "Quarter",
    t.ClinicalRegion AS "ClinicalRegion",
    CASE
        WHEN t.threshold = 7 THEN '% of CSRs w/ ≥1 Incremental Surgeon (≥7 cases)'
        WHEN t.threshold = 13 THEN '% of CSRs w/ ≥1 Incremental Surgeon (≥13 cases)'
    END AS "Type",
    TO_DECIMAL(
        CASE
            WHEN t.csr_cnt = 0 THEN NULL
            ELSE CAST(h.csr_hit_cnt AS FLOAT) / t.csr_cnt
        END,
        15,
        2
    ) AS "Volume",
    'YoY incremental surgeons per CSR (latest-date account mapping)' AS "Description"
FROM
    (
        /* 분모: 분기별 전체 CSR 수 (Open 제외) × 임계값 두 종류(7,13) 확장 */
        SELECT
            q.year,
            q.quarter,
            q.ClinicalRegion,
            th.threshold,
            COUNT(DISTINCT q.histcsr) AS csr_cnt
        FROM
            (
                /* 분기별 account→CSR 매핑 */
                SELECT
                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                    acc.ClinicalRegion,
                    ha.accountguid,
                    ha.histcsr
                FROM
                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                WHERE
                    acc.ClinicalRegion = 'Asia : Korea'
                    AND ha.histcsr IS NOT NULL
                    AND LOWER(ha.histcsr) NOT LIKE '%open%'
            ) q
            CROSS JOIN (
                SELECT
                    7 AS threshold
                UNION
                ALL
                SELECT
                    13 AS threshold
            ) th
        GROUP BY
            q.year,
            q.quarter,
            q.ClinicalRegion,
            th.threshold
    ) t
    LEFT JOIN (
        /* 분자: 해당 분기에 '증분 ≥ threshold' Surgeon을 **최소 2명** 보유한 CSR 수 */
        SELECT
            s.year,
            s.quarter,
            s.ClinicalRegion,
            s.threshold,
            COUNT(DISTINCT s.histcsr) AS csr_hit_cnt
        FROM
            (
                /* CSR별로 임계 충족 surgeon 수를 세고, 2명 이상인 CSR만 추출 */
                SELECT
                    cur.year,
                    cur.quarter,
                    cur.ClinicalRegion,
                    cur.histcsr,
                    th.threshold
                FROM
                    (
                        /* (변경 없음) 분기 내 최신 proceduredatelocal 기준 Surgeon→Account→CSR 매핑 */
                        SELECT
                            rep.year,
                            rep.quarter,
                            rep.ClinicalRegion,
                            rep.surgeonguid,
                            rep.accountguid,
                            qm.histcsr
                        FROM
                            (
                                SELECT
                                    YEAR(p.proceduredatelocal) AS year,
                                    QUARTER(p.proceduredatelocal) AS quarter,
                                    a.ClinicalRegion,
                                    p.surgeonguid,
                                    a.accountguid,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY YEAR(p.proceduredatelocal),
                                        QUARTER(p.proceduredatelocal),
                                        p.surgeonguid
                                        ORDER BY
                                            p.proceduredatelocal DESC,
                                            p.casenumber DESC,
                                            a.accountguid
                                    ) AS rn
                                FROM
                                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
                                WHERE
                                    p.status = 'Completed'
                                    AND a.recordtype = 'Hospital'
                                    AND a.ClinicalRegion = 'Asia : Korea'
                            ) rep
                            LEFT JOIN (
                                SELECT
                                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                                    ha.accountguid,
                                    ha.histcsr
                                FROM
                                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                                WHERE
                                    ha.histcsr IS NOT NULL
                                    AND LOWER(ha.histcsr) NOT LIKE '%open%'
                            ) qm ON qm.year = rep.year
                            AND qm.quarter = rep.quarter
                            AND qm.accountguid = rep.accountguid
                        WHERE
                            rep.rn = 1
                    ) cur
                    JOIN (
                        /* (변경 없음) Surgeon×Account×Quarter 전년동기 증분 */
                        SELECT
                            ba.year,
                            ba.quarter,
                            ba.ClinicalRegion,
                            ba.accountguid,
                            ba.surgeonguid,
                            ba.proc_count - COALESCE(
                                LAG(ba.proc_count, 4) OVER (
                                    PARTITION BY ba.surgeonguid,
                                    ba.accountguid
                                    ORDER BY
                                        (ba.year * 10 + ba.quarter)
                                ),
                                0
                            ) AS proc_incr
                        FROM
                            (
                                SELECT
                                    YEAR(p.proceduredatelocal) AS year,
                                    QUARTER(p.proceduredatelocal) AS quarter,
                                    a.ClinicalRegion,
                                    p.accountguid,
                                    p.surgeonguid,
                                    COUNT(p.recordid) AS proc_count
                                FROM
                                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
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
                            ) ba
                    ) bi ON bi.year = cur.year
                    AND bi.quarter = cur.quarter
                    AND bi.ClinicalRegion = cur.ClinicalRegion
                    AND bi.accountguid = cur.accountguid
                    AND bi.surgeonguid = cur.surgeonguid
                    CROSS JOIN (
                        SELECT
                            7 AS threshold
                        UNION
                        ALL
                        SELECT
                            13 AS threshold
                    ) th
                WHERE
                    cur.histcsr IS NOT NULL
                    AND bi.proc_incr >= th.threshold
                GROUP BY
                    cur.year,
                    cur.quarter,
                    cur.ClinicalRegion,
                    cur.histcsr,
                    th.threshold
                HAVING
                    COUNT(DISTINCT cur.surgeonguid) >= 1 -- ★ 최소 증분 Surgeon 수 = 1
            ) s
        GROUP BY
            s.year,
            s.quarter,
            s.ClinicalRegion,
            s.threshold
    ) h ON h.year = t.year
    AND h.quarter = t.quarter
    AND h.ClinicalRegion = t.ClinicalRegion
    AND h.threshold = t.threshold
UNION
ALL
/* % of CSRs w/ ≥1 Incremental Surgeon (≥7 / ≥13) — latest-date account mapping */
SELECT
    'Surgeon Evolution' AS "Category",
    t.year AS "Year",
    t.quarter AS "Quarter",
    t.ClinicalRegion AS "ClinicalRegion",
    CASE
        WHEN t.threshold = 7 THEN '% of CSRs w/ ≥2 Incremental Surgeon (≥7 cases)'
        WHEN t.threshold = 13 THEN '% of CSRs w/ ≥2 Incremental Surgeon (≥13 cases)'
    END AS "Type",
    TO_DECIMAL(
        CASE
            WHEN t.csr_cnt = 0 THEN NULL
            ELSE CAST(h.csr_hit_cnt AS FLOAT) / t.csr_cnt
        END,
        15,
        2
    ) AS "Volume",
    'YoY incremental surgeons per CSR (latest-date account mapping)' AS "Description"
FROM
    (
        /* 분모: 분기별 전체 CSR 수 (Open 제외) × 임계값 두 종류(7,13) 확장 */
        SELECT
            q.year,
            q.quarter,
            q.ClinicalRegion,
            th.threshold,
            COUNT(DISTINCT q.histcsr) AS csr_cnt
        FROM
            (
                /* 분기별 account→CSR 매핑 */
                SELECT
                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                    acc.ClinicalRegion,
                    ha.accountguid,
                    ha.histcsr
                FROM
                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                WHERE
                    acc.ClinicalRegion = 'Asia : Korea'
                    AND ha.histcsr IS NOT NULL
                    AND LOWER(ha.histcsr) NOT LIKE '%open%'
            ) q
            CROSS JOIN (
                SELECT
                    7 AS threshold
                UNION
                ALL
                SELECT
                    13 AS threshold
            ) th
        GROUP BY
            q.year,
            q.quarter,
            q.ClinicalRegion,
            th.threshold
    ) t
    LEFT JOIN (
        /* 분자: 해당 분기에 '증분 ≥ threshold' Surgeon을 **최소 2명** 보유한 CSR 수 */
        SELECT
            s.year,
            s.quarter,
            s.ClinicalRegion,
            s.threshold,
            COUNT(DISTINCT s.histcsr) AS csr_hit_cnt
        FROM
            (
                /* CSR별로 임계 충족 surgeon 수를 세고, 2명 이상인 CSR만 추출 */
                SELECT
                    cur.year,
                    cur.quarter,
                    cur.ClinicalRegion,
                    cur.histcsr,
                    th.threshold
                FROM
                    (
                        /* (변경 없음) 분기 내 최신 proceduredatelocal 기준 Surgeon→Account→CSR 매핑 */
                        SELECT
                            rep.year,
                            rep.quarter,
                            rep.ClinicalRegion,
                            rep.surgeonguid,
                            rep.accountguid,
                            qm.histcsr
                        FROM
                            (
                                SELECT
                                    YEAR(p.proceduredatelocal) AS year,
                                    QUARTER(p.proceduredatelocal) AS quarter,
                                    a.ClinicalRegion,
                                    p.surgeonguid,
                                    a.accountguid,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY YEAR(p.proceduredatelocal),
                                        QUARTER(p.proceduredatelocal),
                                        p.surgeonguid
                                        ORDER BY
                                            p.proceduredatelocal DESC,
                                            p.casenumber DESC,
                                            a.accountguid
                                    ) AS rn
                                FROM
                                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
                                WHERE
                                    p.status = 'Completed'
                                    AND a.recordtype = 'Hospital'
                                    AND a.ClinicalRegion = 'Asia : Korea'
                            ) rep
                            LEFT JOIN (
                                SELECT
                                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                                    ha.accountguid,
                                    ha.histcsr
                                FROM
                                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                                WHERE
                                    ha.histcsr IS NOT NULL
                                    AND LOWER(ha.histcsr) NOT LIKE '%open%'
                            ) qm ON qm.year = rep.year
                            AND qm.quarter = rep.quarter
                            AND qm.accountguid = rep.accountguid
                        WHERE
                            rep.rn = 1
                    ) cur
                    JOIN (
                        /* (변경 없음) Surgeon×Account×Quarter 전년동기 증분 */
                        SELECT
                            ba.year,
                            ba.quarter,
                            ba.ClinicalRegion,
                            ba.accountguid,
                            ba.surgeonguid,
                            ba.proc_count - COALESCE(
                                LAG(ba.proc_count, 4) OVER (
                                    PARTITION BY ba.surgeonguid,
                                    ba.accountguid
                                    ORDER BY
                                        (ba.year * 10 + ba.quarter)
                                ),
                                0
                            ) AS proc_incr
                        FROM
                            (
                                SELECT
                                    YEAR(p.proceduredatelocal) AS year,
                                    QUARTER(p.proceduredatelocal) AS quarter,
                                    a.ClinicalRegion,
                                    p.accountguid,
                                    p.surgeonguid,
                                    COUNT(p.recordid) AS proc_count
                                FROM
                                    "EDW"."PROCEDURES"."VW_PROCEDURES" p
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
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
                            ) ba
                    ) bi ON bi.year = cur.year
                    AND bi.quarter = cur.quarter
                    AND bi.ClinicalRegion = cur.ClinicalRegion
                    AND bi.accountguid = cur.accountguid
                    AND bi.surgeonguid = cur.surgeonguid
                    CROSS JOIN (
                        SELECT
                            7 AS threshold
                        UNION
                        ALL
                        SELECT
                            13 AS threshold
                    ) th
                WHERE
                    cur.histcsr IS NOT NULL
                    AND bi.proc_incr >= th.threshold
                GROUP BY
                    cur.year,
                    cur.quarter,
                    cur.ClinicalRegion,
                    cur.histcsr,
                    th.threshold
                HAVING
                    COUNT(DISTINCT cur.surgeonguid) >= 2 -- ★ 최소 증분 Surgeon 수 = 2
            ) s
        GROUP BY
            s.year,
            s.quarter,
            s.ClinicalRegion,
            s.threshold
    ) h ON h.year = t.year
    AND h.quarter = t.quarter
    AND h.ClinicalRegion = t.ClinicalRegion
    AND h.threshold = t.threshold
union
all
/* VS/SS Bubble+ YoY Growth — % of CSRs w/ 1 Incremental BB+ (VS or SS 7+) */
SELECT
    'Surgeon Evolution' AS "Category",
    t.year AS "Year",
    t.quarter AS "Quarter",
    t.ClinicalRegion AS "ClinicalRegion",
    'VS/SS Bubble+ YoY Growth' AS "Type",
    CASE
        WHEN t.csr_cnt = 0 THEN TO_DECIMAL(h.csr_hit_cnt, 15, 1)
        ELSE TO_DECIMAL(h.csr_hit_cnt :: FLOAT / t.csr_cnt, 15, 2)
    END AS "Volume",
    '1 Incremental # BB+ with VS/SS(7+)/ # CSR' AS "Description"
FROM
    (
        /* (A) 분기×지역 전체 CSR 모수 */
        SELECT
            q.year,
            q.quarter,
            q.ClinicalRegion,
            COUNT(DISTINCT q.histcsr) AS csr_cnt
        FROM
            (
                /* QBR 매핑: 분기별 accountguid ↔ CSR */
                SELECT
                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                    acc.ClinicalRegion,
                    ha.accountguid,
                    ha.histcsr
                FROM
                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                WHERE
                    acc.ClinicalRegion = 'Asia : Korea'
                    AND ha.histcsr IS NOT NULL
                    AND LOWER(ha.histcsr) NOT LIKE '%open%'
            ) q
        GROUP BY
            q.year,
            q.quarter,
            q.ClinicalRegion
    ) t
    LEFT JOIN (
        /* (B) 증분≥1 VS/SS Bubble+ 계정을 보유한 CSR 수 */
        SELECT
            z.year,
            z.quarter,
            z.ClinicalRegion,
            COUNT(DISTINCT z.histcsr) AS csr_hit_cnt
        FROM
            (
                /* 증분≥1 보유 CSR(distinct) */
                SELECT
                    DISTINCT qm.year,
                    qm.quarter,
                    qm.ClinicalRegion,
                    qm.histcsr
                FROM
                    (
                        /* QBR 매핑 재사용(인라인) */
                        SELECT
                            TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                            TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                            acc.ClinicalRegion,
                            ha.accountguid,
                            ha.histcsr
                        FROM
                            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                        WHERE
                            acc.ClinicalRegion = 'Asia : Korea'
                            AND ha.histcsr IS NOT NULL
                            AND LOWER(ha.histcsr) NOT LIKE '%open%'
                    ) qm
                    JOIN (
                        /* 전년동기 대비 계정당 VS/SS Bubble+ 합계 증분 계산 */
                        SELECT
                            ba.year,
                            ba.quarter,
                            ba.ClinicalRegion,
                            ba.accountguid,
                            (
                                ba.bubble_sum - LAG(ba.bubble_sum, 4) OVER (
                                    PARTITION BY ba.accountguid
                                    ORDER BY
                                        (ba.year * 10 + ba.quarter)
                                )
                            ) AS bubble_incr
                        FROM
                            (
                                /* 계정×분기: Surgeon별 VS/SS Bubble+ 플래그(≥7) 합계 */
                                SELECT
                                    bs.year,
                                    bs.quarter,
                                    bs.ClinicalRegion,
                                    bs.accountguid,
                                    SUM(bs.bubble_flag) AS bubble_sum
                                FROM
                                    (
                                        /* Surgeon×Account×Quarter: VS/SS 사용 케이스 기준 Bubble+ 판정 */
                                        SELECT
                                            YEAR(p.localproceduredate) AS year,
                                            QUARTER(p.localproceduredate) AS quarter,
                                            a.ClinicalRegion AS ClinicalRegion,
                                            p.accountguid,
                                            p.surgeonguid,
                                            CASE
                                                WHEN COUNT(
                                                    CASE
                                                        WHEN (
                                                            p.vesselsealerused = 'Y'
                                                            OR p.synchrosealused = 'Y'
                                                        ) THEN p.casenumber
                                                    END
                                                ) >= 7 THEN 1
                                                ELSE 0
                                            END AS bubble_flag
                                        FROM
                                            EDW.PROCEDURES.VW_PROCEDURESUMMARY p
                                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
                                        WHERE
                                            p.casestatus = 'Completed'
                                            AND a.recordtype = 'Hospital'
                                            AND a.ClinicalRegion = 'Asia : Korea'
                                        GROUP BY
                                            YEAR(p.localproceduredate),
                                            QUARTER(p.localproceduredate),
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
                    ) bi ON bi.year = qm.year
                    AND bi.quarter = qm.quarter
                    AND bi.ClinicalRegion = qm.ClinicalRegion
                    AND bi.accountguid = qm.accountguid
                WHERE
                    bi.bubble_incr > 0
            ) z
        GROUP BY
            z.year,
            z.quarter,
            z.ClinicalRegion
    ) h ON h.year = t.year
    AND h.quarter = t.quarter
    AND h.ClinicalRegion = t.ClinicalRegion
UNION
ALL
/* VS/SS, Stapler, SI Bubble+ YoY Growth — % of CSRs w/ 1 Incremental BB+ (any of VS/SS/Stapler/SI 7+) */
SELECT
    'Surgeon Evolution' AS "Category",
    t.year AS "Year",
    t.quarter AS "Quarter",
    t.ClinicalRegion AS "ClinicalRegion",
    'VS/S,Stapler/SI Bubble+ YoY Growth' AS "Type",
    CASE
        WHEN t.csr_cnt = 0 THEN TO_DECIMAL(h.csr_hit_cnt, 15, 1)
        ELSE TO_DECIMAL(h.csr_hit_cnt :: FLOAT / t.csr_cnt, 15, 2)
    END AS "Volume",
    '1 Incremental # BB+ with Adv.tech(7+)/ # CSR' AS "Description"
FROM
    (
        /* (A) 분기×지역 전체 CSR 모수 */
        SELECT
            q.year,
            q.quarter,
            q.ClinicalRegion,
            COUNT(DISTINCT q.histcsr) AS csr_cnt
        FROM
            (
                /* QBR 매핑: 분기별 accountguid ↔ CSR */
                SELECT
                    TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                    TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                    acc.ClinicalRegion,
                    ha.accountguid,
                    ha.histcsr
                FROM
                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                WHERE
                    acc.ClinicalRegion = 'Asia : Korea'
                    AND ha.histcsr IS NOT NULL
                    AND LOWER(ha.histcsr) NOT LIKE '%open%'
            ) q
        GROUP BY
            q.year,
            q.quarter,
            q.ClinicalRegion
    ) t
    LEFT JOIN (
        /* (B) 증분≥1 Adv.Tech Bubble+ 계정을 보유한 CSR 수 */
        SELECT
            z.year,
            z.quarter,
            z.ClinicalRegion,
            COUNT(DISTINCT z.histcsr) AS csr_hit_cnt
        FROM
            (
                /* 증분≥1 보유 CSR(distinct) */
                SELECT
                    DISTINCT qm.year,
                    qm.quarter,
                    qm.ClinicalRegion,
                    qm.histcsr
                FROM
                    (
                        /* QBR 매핑 재사용(인라인) */
                        SELECT
                            TO_NUMBER(SUBSTRING(ha.yearquarter, 1, 4)) AS year,
                            TO_NUMBER(RIGHT(ha.yearquarter, 1)) AS quarter,
                            acc.ClinicalRegion,
                            ha.accountguid,
                            ha.histcsr
                        FROM
                            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" acc ON acc.accountguid = ha.accountguid
                        WHERE
                            acc.ClinicalRegion = 'Asia : Korea'
                            AND ha.histcsr IS NOT NULL
                            AND LOWER(ha.histcsr) NOT LIKE '%open%'
                    ) qm
                    JOIN (
                        /* 전년동기 대비 계정당 Adv.Tech Bubble+ 합계 증분 계산 */
                        SELECT
                            ba.year,
                            ba.quarter,
                            ba.ClinicalRegion,
                            ba.accountguid,
                            (
                                ba.bubble_sum - LAG(ba.bubble_sum, 4) OVER (
                                    PARTITION BY ba.accountguid
                                    ORDER BY
                                        (ba.year * 10 + ba.quarter)
                                )
                            ) AS bubble_incr
                        FROM
                            (
                                /* 계정×분기: Surgeon별 Adv.Tech Bubble+ 플래그(≥7) 합계 */
                                SELECT
                                    bs.year,
                                    bs.quarter,
                                    bs.ClinicalRegion,
                                    bs.accountguid,
                                    SUM(bs.bubble_flag) AS bubble_sum
                                FROM
                                    (
                                        /* Surgeon×Account×Quarter: VS/SS OR Stapler OR SI 사용 케이스 기준 Bubble+ 판정 */
                                        SELECT
                                            YEAR(p.localproceduredate) AS year,
                                            QUARTER(p.localproceduredate) AS quarter,
                                            a.ClinicalRegion AS ClinicalRegion,
                                            p.accountguid,
                                            p.surgeonguid,
                                            CASE
                                                WHEN COUNT(
                                                    CASE
                                                        WHEN (
                                                            p.vesselsealerused = 'Y'
                                                            OR p.synchrosealused = 'Y'
                                                            OR p.staplerused = 'Y'
                                                            OR p.suctionused = 'Y'
                                                        ) THEN p.casenumber
                                                    END
                                                ) >= 7 THEN 1
                                                ELSE 0
                                            END AS bubble_flag
                                        FROM
                                            EDW.PROCEDURES.VW_PROCEDURESUMMARY p
                                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" a ON a.accountguid = p.accountguid
                                        WHERE
                                            p.casestatus = 'Completed'
                                            AND a.recordtype = 'Hospital'
                                            AND a.ClinicalRegion = 'Asia : Korea'
                                        GROUP BY
                                            YEAR(p.localproceduredate),
                                            QUARTER(p.localproceduredate),
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
                    ) bi ON bi.year = qm.year
                    AND bi.quarter = qm.quarter
                    AND bi.ClinicalRegion = qm.ClinicalRegion
                    AND bi.accountguid = qm.accountguid
                WHERE
                    bi.bubble_incr > 0
            ) z
        GROUP BY
            z.year,
            z.quarter,
            z.ClinicalRegion
    ) h ON h.year = t.year
    AND h.quarter = t.quarter
    AND h.ClinicalRegion = t.ClinicalRegion
union
all
SELECT
    'Academics' AS "Category",
    bp.year,
    bp.quarter,
    bp.ClinicalRegion,
    '% Academics with Sustainable Faculty (Growth Bucket)' AS "Type",
    to_decimal(
        CASE
            WHEN bp."Bubble+" = 0
            OR bp."Bubble+" IS NULL THEN academic."Bubble+"
            ELSE academic."Bubble+" / bp."Bubble+"
        END,
        15,
        2
    ) as volume,
    '# Sustainable Surgeon in hospital of which names include school, university or college / # Sustainable Surgeon' as description
FROM
    (
        SELECT
            bp.year,
            bp.quarter,
            bp.ClinicalRegion,
            SUM(bp."Bubble+") AS "Bubble+"
        FROM
            (
                SELECT
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.surgeonguid,
                    CASE
                        WHEN COUNT(p.recordid) >= 13 THEN 1
                        ELSE 0
                    END AS "Bubble+"
                FROM
                    (
                        SELECT
                            YEAR(p.proceduredatelocal) AS year,
                            quarter(p.proceduredatelocal) AS quarter,
                            p.surgeonguid,
                            p.businesscategoryname,
                            p.subject,
                            p.recordid,
                            account.ClinicalRegion
                        FROM
                            "EDW"."PROCEDURES"."VW_PROCEDURES" p
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
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
                    LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON contact.surgeonguid = p.surgeonguid
                WHERE
                    concat(p.businesscategoryname, p.subject) in (
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
                    )
                GROUP BY
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.surgeonguid
            ) bp
        GROUP BY
            bp.year,
            bp.quarter,
            bp.ClinicalRegion
    ) bp
    LEFT JOIN (
        SELECT
            bp.year,
            bp.quarter,
            bp.ClinicalRegion,
            SUM(bp."Bubble+") AS "Bubble+"
        FROM
            (
                SELECT
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.surgeonguid,
                    CASE
                        WHEN COUNT(p.recordid) >= 13 THEN 1
                        ELSE 0
                    END AS "Bubble+"
                FROM
                    (
                        SELECT
                            YEAR(p.proceduredatelocal) AS year,
                            quarter(p.proceduredatelocal) AS quarter,
                            p.surgeonguid,
                            p.recordid,
                            p.businesscategoryname,
                            p.subject,
                            account.ClinicalRegion
                        FROM
                            "EDW"."PROCEDURES"."VW_PROCEDURES" p
                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
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
                    LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON contact.surgeonguid = p.surgeonguid
                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = contact.accountguid
                WHERE
                    concat(p.businesscategoryname, p.subject) in (
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
                    )
                    AND (
                        LOWER(account.accountname) like '%school%'
                        OR LOWER(account.accountname) like '%college%'
                        OR LOWER(account.accountname) like '%university%'
                    )
                GROUP BY
                    p.year,
                    p.quarter,
                    p.ClinicalRegion,
                    p.surgeonguid
            ) bp
        GROUP BY
            bp.year,
            bp.quarter,
            bp.ClinicalRegion
    ) academic ON bp.year = academic.year
    AND bp.quarter = academic.quarter
    and bp.ClinicalRegion = academic.ClinicalRegion
union
all
SELECT
    'Academics' AS "Category",
    ags.year,
    ags.quarter,
    ags.ClinicalRegion,
    '% of Growth Surgeons Not ISI Trained' AS "Type",
    to_decimal(
        (
            CASE
                WHEN COUNT(DISTINCT ags.surgeonguid) = 0 THEN COUNT(DISTINCT ags.nottrainedprocedure)
                ELSE COUNT(DISTINCT ags.nottrainedprocedure) / COUNT(DISTINCT ags.surgeonguid)
            END
        ),
        15,
        2
    ) as volume,
    '# Growth Not-Trained Active Surgeon / # Growth Active Surgeon (When a surgeon did procedures, either, before the surgeon did his/her first training of Technology Training Multi-Port as a surgeon or a console, or when ISITrained is not checked and there is no record of Technology Training Multi-Port as a surgeon or a console, the surgeon is classified as a non-trained active surgeon.)' as description
FROM
    (
        SELECT
            YEAR(p.proceduredatelocal) AS year,
            quarter(p.proceduredatelocal) AS quarter,
            p.surgeonguid,
            p.recordid,
            p.casenumber,
            p.businesscategoryname,
            p.subject,
            p.recordid,
            training.trainingyear,
            account.ClinicalRegion,
            CASE
                WHEN date_trunc('quarter', p.proceduredatelocal) < date_trunc('quarter', training.trainingyear) THEN p.surgeonguid
                WHEN training.trainingyear IS NULL
                AND contact.TRAINEDFLAG = FALSE THEN p.surgeonguid
                ELSE NULL
            END as nottrainedprocedure
        FROM
            "EDW"."PROCEDURES"."VW_PROCEDURES" p
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON contact.contactguid = p.surgeonguid
            LEFT JOIN (
                SELECT
                    tr."ContactGUID",
                    tr."CertificationDate" AS "TR100TrainedDate",
                    tr."CertificationDate" AS trainingyear
                FROM
                    (
                        SELECT
                            tr.CERTIFICATIONDATE as "CertificationDate",
                            tr.contact as "ContactGUID",
                            contact.fullname,
                            ROW_NUMBER() OVER (
                                PARTITION BY tr.contact
                                ORDER BY
                                    tr.CERTIFICATIONDATE
                            ) as "rownum"
                        FROM
                            "EDW"."TRAINING"."VW_TRNCERTIFICATION" tr
                            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON tr.contact = contact.contactguid
                        WHERE
                            tr.eventtype IN ('Technology Training Multi-Port')
                            and tr.CERTIFICATIONDATE IS NOT NULL
                            and (
                                tr.role = 'Surgeon'
                                or tr.role is null
                                or tr.role = 'Console'
                            )
                    ) tr
                WHERE
                    tr."rownum" = 1
            ) training ON training."ContactGUID" = contact.contactguid
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
            AND concat(p.businesscategoryname, p.subject) in (
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
            )
    ) ags
GROUP BY
    ags.year,
    ags.quarter,
    ags.ClinicalRegion