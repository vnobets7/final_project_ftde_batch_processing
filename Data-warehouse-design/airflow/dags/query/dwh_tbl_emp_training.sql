DROP TABLE IF EXISTS public.Dim_tbl_emp_training;
CREATE TABLE Dim_tbl_emp_training (
  employeeid int,
  employeeName varchar(255),
  trainingName varchar(255),
  startDate DATE,
  endDate DATE,
  isCurrent varchar(255),
  PRIMARY KEY (employeeid)
);