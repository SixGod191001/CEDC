## Naming Conventions For Cross Stack Reference
### Example:
ProjectName and DBName are input parameters
~~~    
    Export:
      # Naming Conventions: <project name>-<ENV>-<service name>-<type>-<name>
      Name: !Sub '${ProjectName}-${ENV}-glue-database-${DBName}'
~~~

## Cross account deploy
When we want to deploy to another account, jenkins pipeline should assume the role in another account,
the role should have related access to make the changes.