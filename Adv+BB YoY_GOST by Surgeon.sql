SELECT
    DISTINCT qbr.accountguid,
    qbr.accountname,
    qbr.year,
    qbr.quarter,
    qbr.histcsr,
    igsbp.surgeonguid,
    igsbp.surgeonname,
    igsbp.volume
from
    (
        SELECT
            DISTINCT ha.accountguid,
            ha.accountname,
            SUBSTRING(ha.yearquarter, 0, 4) as year,
            right(ha.yearquarter, 1) as quarter,
            ha.histcsr,
            account.ClinicalRegion
        FROM
            "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
        WHERE
            lower(ha.histcsr) not like '%open%'
            and account.clinicalregion = 'Asia : Korea'
        GROUP BY
            ha.accountguid,
            ha.accountname,
            SUBSTRING(ha.yearquarter, 0, 4),
            right(ha.yearquarter, 1),
            ha.histcsr,
            account.ClinicalRegion
    ) qbr
    LEFT JOIN (
        SELECT
            igsbp.year,
            igsbp.quarter,
            igsbp.ClinicalRegion,
            igsbp.histcsr,
            igsbp.accountname,
            igsbp.surgeonguid,
            igsbp.surgeonname,
            igsbp.volume -- CASE WHEN SUM(volume) >= 1 THEN igsbp.histcsr ELSE NULL END AS volume
        FROM
            (
                SELECT
                    base.accountguid,
                    base.accountname,
                    base.year,
                    base.quarter,
                    base.calqtroffset,
                    base.ClinicalRegion,
                    igsbp.histcsr,
                    igsbp.surgeonguid,
                    igsbp.surgeonname,
                    IFNULL(igsbp."Bubble+", 0) - IFNULL(
                        lag(igsbp."Bubble+", 4) over (
                            partition by igsbp.surgeonguid
                            order by
                                igsbp.CALQTROFFSET
                        ),
                        0
                    ) as volume
                FROM
                    (
                        -- base start
                        SELECT
                            account.accountguid,
                            account.accountname,
                            year.year,
                            year.quarter,
                            year.CALQTROFFSET,
                            account.ClinicalRegion
                        FROM
                            (
                                SELECT
                                    DISTINCT SUBSTRING(ha.yearquarter, 0, 4) as year,
                                    right(ha.yearquarter, 1) as quarter,
                                    pr.CALQTROFFSET
                                FROM
                                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                                    left join EDW.PROCEDURES.VW_PROCEDURESUMMARY pr on year(pr.localproceduredate) || '-Q' || quarter(pr.localproceduredate) = ha.yearquarter
                            ) year
                            CROSS JOIN (
                                SELECT
                                    DISTINCT ha.accountguid,
                                    ha.accountname,
                                    account.ClinicalRegion
                                FROM
                                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
                                where
                                    account.clinicalregion = 'Asia : Korea'
                            ) account
                    ) base -- base end
                    LEFT JOIN (
                        SELECT
                            igsbp.accountguid,
                            igsbp.accountname,
                            igsbp.year,
                            igsbp.quarter,
                            igsbp.calqtroffset,
                            igsbp.histcsr,
                            igsbp.ClinicalRegion,
                            igsbp.surgeonguid,
                            igsbp.surgeonname,
                            igsbp."Bubble+"
                        FROM
                            (
                                SELECT
                                    DISTINCT ha.accountguid,
                                    ha.accountname,
                                    SUBSTRING(ha.yearquarter, 0, 4) as year,
                                    right(ha.yearquarter, 1) as quarter,
                                    pr.calqtroffset,
                                    ha.histcsr,
                                    account.ClinicalRegion,
                                    bp.surgeonguid,
                                    bp.surgeonname,
                                    bp."Bubble+"
                                FROM
                                    "EDW"."SALESSHARED"."VW_SALESALIGNMENTHISTORYYEARQUARTER" ha
                                    LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = ha.accountguid
                                    left join EDW.PROCEDURES.VW_PROCEDURESUMMARY pr on year(pr.localproceduredate) || '-Q' || quarter(pr.localproceduredate) = ha.yearquarter
                                    LEFT JOIN (
                                        SELECT
                                            bp.year,
                                            bp.quarter,
                                            bp.ClinicalRegion,
                                            bp.accountguid,
                                            bp.accountname,
                                            bp.surgeonguid,
                                            bp.surgeonname,
                                            SUM(bp."Bubble+") AS "Bubble+"
                                        FROM
                                            (
                                                SELECT
                                                    p.year,
                                                    p.quarter,
                                                    p.ClinicalRegion,
                                                    p.surgeonguid,
                                                    p.surgeonname,
                                                    p.accountguid,
                                                    p.accountname,
                                                    case
                                                        when sum(
                                                            CASE
                                                                WHEN p.staplerused = 'Y'
                                                                or p.vesselsealerused = 'Y'
                                                                or p.synchrosealused = 'Y'
                                                                or p.suctionused = 'Y' then p.runrate
                                                            end
                                                        ) >= 7 THEN 1
                                                        ELSE 0
                                                    END AS "Bubble+"
                                                FROM
                                                    (
                                                        SELECT
                                                            YEAR(p.localproceduredate) AS year,
                                                            quarter(p.localproceduredate) as quarter,
                                                            p.surgeonguid,
                                                            p.surgeonname,
                                                            p.casenumber,
                                                            p.runrate,
                                                            p.staplerused,
                                                            p.suctionused,
                                                            p.vesselsealerused,
                                                            p.synchrosealused,
                                                            p.accountguid,
                                                            p.accountname,
                                                            p.sfdccurrentquarterflag,
                                                            account.ClinicalRegion
                                                        FROM
                                                            EDW.PROCEDURES.VW_PROCEDURESUMMARY p
                                                            LEFT JOIN "EDW"."MASTER"."VW_ACCOUNT" account ON account.accountguid = p.accountguid
                                                            LEFT JOIN "EDW"."MASTER"."VW_CONTACT" contact ON contact.surgeonguid = p.surgeonguid
                                                        WHERE
                                                            p.casestatus = 'Completed'
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
                                                GROUP BY
                                                    p.year,
                                                    p.quarter,
                                                    p.ClinicalRegion,
                                                    p.surgeonguid,
                                                    p.surgeonname,
                                                    p.accountguid,
                                                    p.accountname,
                                                    p.sfdccurrentquarterflag
                                            ) bp
                                        GROUP BY
                                            bp.year,
                                            bp.quarter,
                                            bp.ClinicalRegion,
                                            bp.accountguid,
                                            bp.accountname,
                                            bp.surgeonguid,
                                            bp.surgeonname
                                    ) bp ON bp.accountguid = ha.accountguid
                                    AND bp.year = SUBSTRING(ha.yearquarter, 0, 4)
                                    and bp.quarter = right(ha.yearquarter, 1)
                                WHERE
                                    ha.histcsr IS NOT NULL
                                    and (lower(ha.histcsr) not like '%open%')
                            ) igsbp
                    ) igsbp ON igsbp.accountguid = base.accountguid
                    AND igsbp.year = base.year
                    AND igsbp.quarter = base.quarter
                    and base.ClinicalRegion = igsbp.ClinicalRegion
            ) igsbp
        GROUP BY
            igsbp.year,
            igsbp.quarter,
            igsbp.calqtroffset,
            igsbp.ClinicalRegion,
            igsbp.histcsr,
            igsbp.accountname,
            igsbp.surgeonguid,
            igsbp.surgeonname,
            igsbp.volume
    ) igsbp ON qbr.year = igsbp.year
    AND qbr.quarter = igsbp.quarter
    and qbr.accountname = igsbp.accountname
    AND qbr.ClinicalRegion = igsbp.ClinicalRegion