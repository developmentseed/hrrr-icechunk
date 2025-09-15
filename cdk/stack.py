from aws_cdk import (
    CustomResource,
    Duration,
    Stack,
    Tags,
)
from aws_cdk import (
    aws_ecr_assets as ecr_assets,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from aws_cdk import (
    aws_lambda_event_sources as lambda_event_sources,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_sns as sns,
)
from aws_cdk import (
    aws_sns_subscriptions as subscriptions,
)
from aws_cdk import (
    aws_sqs as sqs,
)
from aws_cdk import custom_resources as cr
from constructs import Construct
from settings import StackSettings


class HrrrSnsSqsStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, settings: StackSettings, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("Project", settings.PROJECT)

        hrrr_dlq = sqs.Queue(
            self,
            "HrrrNotificationDlq",
            queue_name="hrrr-notifications-dlq",
            retention_period=Duration.days(14),
        )

        hrrr_queue = sqs.Queue(
            self,
            "HrrrNotificationQueue",
            queue_name="hrrr-notifications",
            visibility_timeout=Duration.seconds(1800),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=20,
                queue=hrrr_dlq,
            ),
        )

        icechunk_bucket = s3.Bucket(
            self,
            "IceChunkHrrrBucket",
            bucket_name=settings.ICECHUNK_BUCKET_NAME,
        )

        hrrr_topic = sns.Topic.from_topic_arn(
            self,
            "HrrrSnsTopic",
            topic_arn="arn:aws:sns:us-east-1:123901341784:NewHRRRObject",
        )

        hrrr_topic.add_subscription(
            subscriptions.SqsSubscription(
                hrrr_queue,
                raw_message_delivery=True,
            )
        )

        hrrr_lambda = _lambda.DockerImageFunction(
            self,
            "HrrrProcessorLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
                file="append/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(5),
            memory_size=2048,
            environment={
                "QUEUE_URL": hrrr_queue.queue_url,
            },
        )

        hrrr_queue.grant_consume_messages(hrrr_lambda)

        # Grant Lambda permissions to read from S3 (for processing HRRR files)
        hrrr_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                resources=[
                    "arn:aws:s3:::noaa-hrrr-bdp-pds/*",
                    "arn:aws:s3:::noaa-hrrr-bdp-pds",
                ],
            )
        )

        # Grant Lambda permissions to write to the icechunk S3 bucket
        icechunk_bucket.grant_read_write(hrrr_lambda)

        hrrr_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                hrrr_queue,
                batch_size=10,
                report_batch_item_failures=True,
                max_concurrency=2,
            )
        )

        initialize_icechunk_lambda = _lambda.DockerImageFunction(
            self,
            "InitializeIcechunkLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
                file="initialize/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(5),
            memory_size=2048,
        )

        icechunk_bucket.grant_read_write(initialize_icechunk_lambda)

        custom_resource_provider = cr.Provider(
            self,
            "S3BucketCustomResourceProvider",
            on_event_handler=initialize_icechunk_lambda,
        )

        bucket_custom_resource = CustomResource(
            self,
            "S3BucketCustomResource",
            service_token=custom_resource_provider.service_token,
            properties={
                "BucketName": icechunk_bucket.bucket_name,
            },
        )

        bucket_custom_resource.node.add_dependency(icechunk_bucket)
