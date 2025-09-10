#!/usr/bin/env python3

import aws_cdk as cdk

from hrrr_sns_sqs_stack import HrrrSnsSqsStack

app = cdk.App()
HrrrSnsSqsStack(
    app, 
    "HrrrSns",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region="us-east-1"
    )
)

app.synth()
