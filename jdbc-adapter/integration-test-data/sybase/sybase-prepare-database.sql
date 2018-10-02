USE master go

-- Initialiase a data partition
DISK INIT
  name = 'data_dev1',
  physname = 'data_dev1.dat',
  size = '100M'
go
 
-- Initialize a database log partition
DISK INIT
  name = 'log_dev1',
  physname = 'log_dev1.dat',
  size = '25M'
go

--DROP DATABASE testdb go
CREATE DATABASE testdb ON data_dev1='25M' LOG ON log_dev1='5M' go
sp_addlogin 'tester', 'tester' go