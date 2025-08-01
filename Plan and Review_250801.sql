SELECT
    hist.histcsm,
    hist.histcsd,
    plan.name AS "PlanName",
    plan.OWNERSNAME,
    plan.YEARANDQUARTER,
    goal.goal AS "GoalID",
    tac.TACTICNUMBER,
    mile.accountid,
    mile.accountname,
    mile.surgeonid,
    mile.surgeonname,
    mile.pathwayname,
    mile.RECORDTYPEID,
    mile.RECORDTYPENAME
FROM
    EDW.SALESSHARED.VW_PLANS plan
    INNER JOIN (
        SELECT
            *
        FROM
            SFDC.SFDC.USER
        WHERE
            country = 'South Korea' / / isactive = TRUE
            AND / / department LIKE '%Clinical Sales%'
    ) user
) user ON plan.OWNERID = user.id
LEFT JOIN EDW.SALESSHARED.VW_GOAL goal ON plan.id = goal.planid
LEFT JOIN EDW.SALESSHARED.VW_GOALTACTICS tac ON goal.goalguid = tac.goal
LEFT JOIN EDW.OPPORTUNITY.VW_MILESTONEPATHWAY mile ON tac.pathway = mile.MILESTONEPATHWAYGUID
LEFT JOIN EDW.SALESSHARED.VW_SALESALIGNMENTHISTORYYEARQUARTER hist ON hist.yearquarter = plan.yearandquarter
AND mile.accountguid = hist.accountguid
WHERE
    YEAR(plan.quarterenddate) >= 2024
    AND tac.tacticnumber IS NOT NULL