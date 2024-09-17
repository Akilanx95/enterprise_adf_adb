# Enterprise Data Integration with Azure ADF & Databricks

This project demonstrates a comprehensive enterprise-level data integration solution utilizing Azure Data Factory (ADF) and Azure Databricks (ADB) for transforming and managing large-scale datasets. The architecture incorporates a multi-layered approach to data processing, including bronze, silver, and gold data layers, with seamless integration between different data sources such as Azure Data Lake Storage (ADLS), SQL Server, and SAP HANA.

Key Features:
- Bronze Layer (Raw Data): Handles the ingestion of raw data from various sources into the bronze layer, ensuring that data is captured in its raw form for further processing.
- Silver Layer (Refined Data): Data transformations in the silver layer provide cleaner and more refined datasets, suitable for analytical consumption.
- Parallel Data Pipelines: Leveraging ADF's pipeline parallelism for efficient data loading, transformation, and orchestration, ensuring high performance.
- Dynamic Metadata Management: SQL-based metadata framework used to manage data processing configurations, providing flexibility and scalability across the pipeline.
- Audit & Logging: Robust auditing and logging mechanisms via stored procedures to ensure data integrity and transparency throughout the processing lifecycle.

Components:
- Notebooks: Python-based Databricks notebooks for orchestrating ETL processes across the bronze and silver layers.
- ARM Templates: Infrastructure as code (IaC) templates to deploy and manage Azure resources required for the project.
- SQL Scripts: DDLs, functions, and stored procedures for setting up and managing metadata and audit logs.
- Pipelines: Pre-configured ADF pipelines for executing parallel data processing tasks.

This solution is designed to handle complex data pipelines for large organizations, ensuring scalability, performance, and maintainability through the use of modern cloud-native technologies.
