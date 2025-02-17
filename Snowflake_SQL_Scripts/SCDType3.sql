create or replace Table Employee_Source(
Id number,
Name varchar,
Salary number
);


INSERT INTO Employee_Source (Id, Name, Salary)
VALUES
    (1, 'Alice', 50000),
    (2, 'Bob', 60000),
    (3, 'Charlie', 55000);


CREATE or replace TABLE Employee_Target (
    Id INT ,
    Name VARCHAR(100),
    Salary int,
    Previous_Salary INT,
    Current_Salary INT
)


INSERT INTO Employee_Target (ID, Name, Salary,Previous_Salary,Current_Salary)
VALUES
    (1, 'Alice', 55000, Null,Null),
    (2, 'Bob', 90000, Null,Null),
    (3, 'Charlie', 55000,Null,Null);



Merge into EMPLOYEE_TARGET Et
USING Employee_Source Es
on Et.Id = Es.Id
when matched and (Et.salary <> Es.Salary)
then update set
Et.Previous_Salary = Et.Salary,
Et.Current_Salary = Es.Salary
When not matched then Insert(Id,Name,Salary,Previous_Salary,Current_Salary) values (Es.Id,Es.Name,Es.Salary,Null,Es.Salary)


select * from EMPLOYEE_TARGET