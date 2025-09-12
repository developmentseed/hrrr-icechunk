from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from hrrr import initialize_icechunk

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event, context: LambdaContext):
    try:
        initialize_icechunk() 
        logger.info("Icechunk initialized")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
