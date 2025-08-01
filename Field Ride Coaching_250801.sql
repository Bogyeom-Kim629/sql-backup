SELECT
     fc.*,
     YEAR(fc.field_ride_date) || '-Q' || QUARTER(fc.field_ride_date) AS "Year-Quarter",
     fc.OWNER_NAME as histcsr,
     hist.histcsm,
     hist.histcsd,
     plan.name AS "PlanName",
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
     EDW.TRAINING.VW_FIELD_RIDE_COACHING fc
     INNER JOIN (
          SELECT
               *
          FROM
               SFDC.SFDC.USER
          WHERE
               country = 'South Korea' / / isactive = TRUE
               AND / / department LIKE '%Clinical Sales%'
     ) user
) user ON fc.OWNER_GUID = user.id
LEFT JOIN EDW.SALESSHARED.VW_PLANS plan ON fc.plan = plan.id
LEFT JOIN EDW.SALESSHARED.VW_GOAL goal ON plan.id = goal.planid
LEFT JOIN EDW.SALESSHARED.VW_GOALTACTICS tac ON goal.goalguid = tac.goal
LEFT JOIN EDW.OPPORTUNITY.VW_MILESTONEPATHWAY mile ON tac.pathway = mile.MILESTONEPATHWAYGUID
LEFT JOIN EDW.SALESSHARED.VW_SALESALIGNMENTHISTORYYEARQUARTER hist ON hist.yearquarter = YEAR(fc.field_ride_date) || '-Q' || QUARTER(fc.field_ride_date)
AND hist.histcsr = fc.owner_name
WHERE
     YEAR(fc.field_ride_date) >= 2024
     AND tac.tacticnumber IS NOT NULL