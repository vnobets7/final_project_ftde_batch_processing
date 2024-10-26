DROP TABLE IF EXISTS public.fact_tbl_HR;
CREATE TABLE fact_tbl_HR (
  employeeid int,
  employeeName varchar(255),
  gender varchar(255),
  age int,
  department varchar(255),
  title varchar(255),
  salary int,
  employeeSalary int,
  BonusOverTime int,
  salaryDate Date,
  trainingName varchar(255),
  startDate DATE,
  endDate DATE,
  isCurrent varchar(255),
  trainingReview varchar(255),
  trainingRating float,
  performanceComment varchar(255),
  PRIMARY KEY (employeeid)
)