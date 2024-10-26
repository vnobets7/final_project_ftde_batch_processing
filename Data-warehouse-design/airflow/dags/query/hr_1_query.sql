SELECT dtd.employeeid,
       dtd.employeeName,
       gender,
       age,
       department,
       title,
       employeeSalary,
       BonusOverTime,
       salaryDate,
       trainingName, 
       dtd.startDate,
       dtd.endDate,
       isCurrent,
       trainingReview, 
       trainingRating,
       performanceComment
FROM dim_tbl_emp_salary AS dtes
LEFT JOIN 
     dim_tbl_emp_training_performance_review AS dttp
ON dtes.employeeid = dttp.employeeid;