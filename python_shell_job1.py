
import sys
import boto3
import io
import time


def poll_status(athena_client, execution_id):
    """ Checks the status of the a query using an incoming execution id and returns
    a 'pass' string value when the status is either "SUCCEEDED, FAILED or CANCELLED. """
    
    result = athena_client.get_query_execution(QueryExecutionId=execution_id)
    state  = result['QueryExecution']['Status']['State']
    
    if state == 'SUCCEEDED':
        return 'pass'
    if state == 'FAILED':
        return 'pass'
    if state == 'CANCELLED':
        return 'pass'
    else:
        return 'not pass'
    
def poll_result(athena_client, execution_id):
    """ Gets the query result using an incoming execution id. This function is ran after the 
    poll_status function and only if we are sure that the query was fully executed. """
    
    result = athena_client.get_query_execution(QueryExecutionId=execution_id)
    
    return result

def run_query_get_result(
  athena_client,
  s3_bucket,
  query, 
  database, 
  s3_output, 
  s3_prefix):
    """ Runs an incoming query and returns the output as an s3 file like object.
    """
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
    })

    QueryExecutionId = response.get('QueryExecutionId')
    
    print(QueryExecutionId)
    
    # wait until query is executed
    while poll_status(athena_client, QueryExecutionId) != 'pass':
        time.sleep(2)
        # you can add some error handling when the
        # query failed or was being cancelled
        pass
    
    result = poll_result(athena_client, QueryExecutionId)
    
    r_file_object = None
    
    # only return file like object when the query succeeded 
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        s3_key = s3_prefix + QueryExecutionId + '.csv'
        
        s3 = boto3.resource('s3')
        r_file_object = s3.Object(s3_bucket, s3_key)
        #s3.Bucket(s3_bucket).download_file(s3_key, local_filename)
        
    return r_file_object 
    

SQL_QUERY = """SELECT customeremail FROM customer"""

# r_file_object = 'Hello'
athena_client = boto3.client('athena', region_name='eu-west-1')
s3_bucket = 'hszedx-course-exercise-material'
athena_db = 's3database'
s3_prefix = 'agg_customer'
s3_output = 's3://'+ s3_bucket + '/' + s3_prefix
print(s3_output)
# # run function and get file like object 
r_file_object = run_query_get_result(
    athena_client, 
    s3_bucket,
    SQL_QUERY, 
    athena_db, 
    s3_output, 
    s3_prefix
)

print(r_file_object)
