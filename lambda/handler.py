import json
from pathlib import Path
from typing import Any, Dict

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from hrrr import append_grib

logger = Logger()
tracer = Tracer()
processor = BatchProcessor(event_type=EventType.SQS)


@tracer.capture_method
def record_handler(record: SQSRecord) -> None:
    """
    Process individual SQS record.
    
    Args:
        record: SQS record from the batch
    """
    try:
        # Extract message body
        message_body = record.body
        
        # Parse the SNS message if it's from SNS
        message = json.loads(message_body)
        
        # If message is from SNS, extract the actual message
        if "Message" in message:
            sns_message = json.loads(message["Message"])
            process_hrrr_notification(sns_message)
        else:
            # Direct SQS message
            process_hrrr_notification(message)
            
    except Exception as e:
        logger.error(
            f"Error processing record: {str(e)}",
            extra={"message_id": record.message_id}
        )
        raise


@tracer.capture_method
def process_hrrr_notification(message: Dict[str, Any]) -> None:
    """
    Process a HRRR notification message.
    
    Args:
        message: The HRRR notification message to process
    """
    # Extract key information from the message
    bucket = message.get("Records", [{}])[0].get("s3", {}).get("bucket", {}).get("name")
    key = message.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key")
    if key and bucket:
        ext = Path(key).suffix.lstrip(".") 
        hrrr_types = ["wrfsfc"]
        if any(s in key for s in hrrr_types) \
                and ext == "grib2" and ".ak" not in key:
            logger.info(
                "HRRR file available",
                extra={
                    "bucket": bucket, 
                    "key": key, 
                    "s3_uri": f"s3://{bucket}/{key}"}
            )
            append_grib(bucket=bucket, key=key)
            logger.info(f"{key} successfully appended")


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event, context: LambdaContext):
    """
    Lambda function to process HRRR notification messages from SQS queue.
    
    Args:
        event: Lambda event containing SQS records
        context: Lambda context object
        
    """
    return process_partial_response(
        event=event,
        record_handler=record_handler,
        processor=processor,
        context=context,
    )

