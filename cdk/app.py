#!/usr/bin/env python3

import aws_cdk as cdk
from settings import StackSettings  # type: ignore[import-not-found]
from stack import HrrrSnsSqsStack  # type: ignore[import-not-found]

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
