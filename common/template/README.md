## Naming Conventions For Cross Stack Reference
### Example:
ProjectName and DBName are input parameters
~~~    
    Export:
      # Naming Conventions: <project name>-<ENV>-<service name>-<type>-<name>
      Name: !Sub '${ProjectName}-${ENV}-glue-database-${DBName}'
~~~