SELECT dtd.employeeid,
       dtd.name AS employeeName,
       dtd.trainingProgram AS trainingName, 
       dtd.startDate,
       dtd.endDate,
       dtd.status AS isCurrent,
       dpm.reviewPeriod AS trainingReview, 
       dpm.rating AS trainingRating,
       dpm.comments AS performanceComment
FROM data_training_development AS dtd
LEFT JOIN 
     data_performance_management AS dpm
ON dtd.employeeid = dpm.employeeid;