## Architecture

![Architecture](https://github.com/SixGod191001/CEDC/blob/main/databricks%20workspace/doc/DataBricks_Architecture.png)

## databricks中的工程目录
+ Workspace
  + Users
    + class+009@databricks.com
      + Trash
      + CEDC_Workspace
        + BronzeLayer
          ++ Bronze_Common_Inc(notebook)
          ++ Bronze_NB_2B_DIM_PARTY_HIST(notebook)
        + SliverLayer
          ++ Sliver_Dim_NB_2B_Party_Trans(notebook)
          ++ Sliver_Dim_NB_2B_Party_Merge(notebook)
        + GoldLayer
          ++ Gold_NB
        + utils
        + includes(待定，和utils二选一)
+ data
  + cedc(catalog)
    + cedc_schema
      + sliver_dim_nb_2b_party
      + t_b_dim_party_demo
      + v_s_dim_party_hosp
+ Compute
  + class+009@databricks.com's Cluster
+ Wolkflows
  + jobs
    + JB_B2S_DIM_ALL
      + Taks
        + TK_B2S_DIM_PARTY
        + TK_S2G_DIM_PARTY
  + job runs
  + Delta Live Tables
    + PIP_B2S_DIM_PARTY

## 命名规范
### 数据库对象：
+ 示例：  b_dim_party_demo
+ 结构：  {{层级}\_{dim/fct}\_{业务自定义部分(\_类型})?}
+     类型={t:table,v:view,lt:live table,tv:temp view,f:udf}
+     层级={b：bronze,s:Silver,g:Gold}

### databrick其它对象：
+ 示例： JB_B2S_DIM_ALL;TK_B2S_DIM_PARTY;NB_2B_DIM_PARTY
+ 结构： {对象类型}\_{来源层}2{目标层}\_{dim/fct}\_{目标表名业务部分及其它} 
+     类型={JB:job,TK:Task,NB:notebook,PIP:pipeline}

### 其它约定
+ 一个notebook中只更新一个目标表
+ coding层，变量名全小写
+ 按照Python规则编写， 不用缺省值，明确指出magic command
+ 导出选第一个类型
+ 考虑给aiflow留接口



## Reference Docs:

### [什么是数据湖？](https://aws.amazon.com/cn/big-data/datalakes-and-analytics/what-is-a-data-lake/)

### [使用 AWS Glue 和 Amazon S3 构建数据湖基础](https://aws.amazon.com/cn/blogs/china/use-aws-glue-amazon-s3-build-datalake/)

### [Best Practices and Guidance for Cloud Engineers to Deploy Databricks on AWS: Part 1](https://www.databricks.com/blog/2022/09/30/best-practices-and-guidance-cloud-engineers-deploy-databricks-aws-part-1.html#:~:text=Databricks%20architecture%20for%20cloud%20engineers%201%20The%20control,that%20are%20spun%20up%20within%20your%20AWS%20environment.)

### [Best Practices and Guidance for Cloud Engineers to Deploy Databricks on AWS: Part 2](https://www.databricks.com/blog/2023/01/27/best-practices-and-guidance-cloud-engineers-deploy-databricks-aws-part-2.html)

### [Optimizing AWS S3 Access for Databricks](https://www.databricks.com/blog/2022/11/08/optimizing-aws-s3-access-databricks.html)

