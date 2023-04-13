p align="center"><img src="img/gram.png")<p>

# 1 - Batch data ingestion pipeline 

### 1.1 The ingestion will be applied to files of data. Define a set of constraints for files that mysimbdp will support for ingestion. Design a configuration model for the tenant service profile that can be used to specify a set of constraints for ingestion (e.g., maximum number of files and amount of data). Explain why you as a platform provider decide such constraints. Implement these constraints into simple configuration files and provide examples (e.g., JSON or YAML)

Service profile is defined in two .xml files for input and ouptut of the clientBatchIngestApp, One limits the number of files (per hour) and file extension that can be accepted and forwarded into clientBatchIngestApp, the other sets a constraint on size of data per hour that is allowed for ingestion from clientBatchIngestApp to the database. As a service platform provider we want to have control over disk access in StagingInputDirectory and reads, we also want to differentiate the client services based on their pay, for example tenant1 pays little and tenant2 pays alot for his service, this comes at a for mysimbdp, therefore we want to limit the amount of files and data that each tenant can process, depending on their service profile. Cheching file format and limiting uploadrates can also provide some safety to the system. 

For testing for each tenant (two tenants) a personal batch ingestion service profile has been developed, the first one limits user to stage 500 files of any format for ingestion per-hour and 1 Gigabyte per-hour, the second one limits user to stage 500 .csv files for ingestion per-hour and 100 Gigabytes per-hour. It is also possible to limit the file size:
```
<entry>
<key>Maximum File Size</key>
  <value>1 GB</value>
</entry>
```
