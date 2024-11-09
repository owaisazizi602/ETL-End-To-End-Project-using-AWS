import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3_data_source
S3_data_source_node1731154893945 = glueContext.create_dynamic_frame.from_catalog(database="movie_db", table_name="input", transformation_ctx="S3_data_source_node1731154893945")

# Script generated for node  Data Quality checks
DataQualitychecks_node1731154975591_ruleset = """

    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
     
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
        


    ]
"""

DataQualitychecks_node1731154975591 = EvaluateDataQuality().process_rows(frame=S3_data_source_node1731154893945, ruleset=DataQualitychecks_node1731154975591_ruleset, publishing_options={"dataQualityEvaluationContext": "DataQualitychecks_node1731154975591", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1731155247502 = SelectFromCollection.apply(dfc=DataQualitychecks_node1731154975591, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1731155247502")

# Script generated for node ruleOutcomes
ruleOutcomes_node1731155190399 = SelectFromCollection.apply(dfc=DataQualitychecks_node1731154975591, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1731155190399")

# Script generated for node Conditional Router
ConditionalRouter_node1731155629126 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1731155247502,
  group_filters = [GroupFilter(name = "Failed_Records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node Failed_Records
Failed_Records_node1731155629395 = SelectFromCollection.apply(dfc=ConditionalRouter_node1731155629126, key="Failed_Records", transformation_ctx="Failed_Records_node1731155629395")

# Script generated for node default_group
default_group_node1731155629363 = SelectFromCollection.apply(dfc=ConditionalRouter_node1731155629126, key="default_group", transformation_ctx="default_group_node1731155629363")

# Script generated for node Drop_Column
Drop_Column_node1731155988567 = ApplyMapping.apply(frame=default_group_node1731155629363, mappings=[("overview", "string", "overview", "string"), ("gross", "string", "gross", "string"), ("director", "string", "director", "string"), ("certificate", "string", "certificate", "string"), ("star4", "string", "star4", "string"), ("runtime", "string", "runtime", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("no_of_votes", "long", "no_of_votes", "int"), ("series_title", "string", "series_title", "string"), ("meta_score", "long", "meta_score", "int"), ("star1", "string", "star1", "string"), ("genre", "string", "genre", "string"), ("released_year", "string", "released_year", "string"), ("poster_link", "string", "poster_link", "string"), ("imdb_rating", "double", "imdb_rating", "decimal")], transformation_ctx="Drop_Column_node1731155988567")

# Script generated for node Rule_outcome
Rule_outcome_node1731155488947 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1731155190399, connection_type="s3", format="json", connection_options={"path": "s3://moviesdatanalysis/rule_outcome/", "partitionKeys": []}, transformation_ctx="Rule_outcome_node1731155488947")

# Script generated for node Failed_Into_S3
Failed_Into_S3_node1731155840578 = glueContext.write_dynamic_frame.from_options(frame=Failed_Records_node1731155629395, connection_type="s3", format="json", connection_options={"path": "s3://moviesdatanalysis/Failed_Records/", "partitionKeys": []}, transformation_ctx="Failed_Into_S3_node1731155840578")

# Script generated for node Redshift_Load
Redshift_Load_node1731156516951 = glueContext.write_dynamic_frame.from_catalog(frame=Drop_Column_node1731155988567, database="movie_db", table_name="dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://moviesdatanalysis/temp/",additional_options={"aws_iam_role": "arn:aws:iam::209479286927:role/redshift123"}, transformation_ctx="Redshift_Load_node1731156516951")

job.commit()