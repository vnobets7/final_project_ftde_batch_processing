DROP TABLE IF EXISTS public.Dim_tbl_training_development;
CREATE TABLE Dim_tbl_training_development (
  trainingid serial PRIMARY key not null,
  trainingName varchar(255), 
  startDate Date,
  endDate Date
);