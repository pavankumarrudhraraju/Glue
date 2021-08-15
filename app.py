#!/usr/bin/env python3

import os

from aws_cdk import core as cdk

from aws_cdk import core

from glue.glue_stack import GlueStack

_env=core.Environment(region='ap-south-1')

app = core.App()

GlueStack(app, "GlueStack",env=_env,)

app.synth()
