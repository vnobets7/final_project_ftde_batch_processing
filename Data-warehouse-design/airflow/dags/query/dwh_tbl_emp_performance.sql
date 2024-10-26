DROP TABLE IF EXISTS public.Dim_tbl_emp_performance;
CREATE TABLE Dim_tbl_emp_performance (
  employeeid int,
  employeeName varchar(255),
  trainingReview varchar(255),
  trainingRating float,
  performanceComment varchar(255),
  PRIMARY KEY (employeeid)
);