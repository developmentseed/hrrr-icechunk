#!/usr/bin/env python3

import aws_cdk as cdk
from settings import StackSettings
from stack import HrrrSnsSqsStack

settings = StackSettings()

app = cdk.App()
HrrrSnsSqsStack(
    app,
    settings.STACK_NAME,
    settings=settings,
    env=cdk.Environment(
        account=app.node.try_get_context("account"), region=settings.ACCOUNT_REGION
    ),
)

app.synth()
