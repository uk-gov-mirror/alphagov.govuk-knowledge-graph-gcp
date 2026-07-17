class JobFailure(Exception):
    """
    Custom exception raised when the pipeline job fails.
    Used to signal controlled failure in ETL pipeline execution.
    """
    pass