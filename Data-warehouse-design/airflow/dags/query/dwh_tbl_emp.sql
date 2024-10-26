DROP TABLE IF EXISTS public.dim_tbl_emp;
CREATE TABLE dim_tbl_emp (
  employeeid int,
  employeeName varchar(255), 
  gender varchar(255),
  age int,
  department varchar(255),
  title varchar(255),
  PRIMARY KEY (employeeid)
);
INSERT INTO dim_tbl_emp VALUES %s;