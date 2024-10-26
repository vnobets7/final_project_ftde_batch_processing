DROP TABLE IF EXISTS public.dim_tbl_job_salary;
CREATE TABLE Dim_tbl_job_salary (
  job_id serial PRIMARY key not null,
  title varchar(255),
  department varchar(255),
  salary int
);