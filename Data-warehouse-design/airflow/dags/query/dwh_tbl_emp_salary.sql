DROP TABLE IF EXISTS public.Dim_tbl_emp_salary;
CREATE TABLE Dim_tbl_emp_salary (
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
  PRIMARY KEY (employeeid)
);