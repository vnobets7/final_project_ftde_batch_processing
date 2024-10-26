DROP TABLE IF EXISTS public.Dim_tbl_emp_training_performance_review;
CREATE TABLE Dim_tbl_emp_training_performance_review (
  employeeid int,
  employeeName varchar(255),
  trainingName varchar(255),
  startDate DATE,
  endDate DATE,
  isCurrent varchar(255),
  trainingReview varchar(255),
  trainingRating float,
  performanceComment varchar(255),
  PRIMARY KEY (employeeid)
);