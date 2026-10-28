import aws_cdk as cdk

from stack import ShadowTalkStack


app = cdk.App()
ShadowTalkStack(
    app,
    "ShadowTalkDemo",
    env=cdk.Environment(account=app.node.try_get_context("account"), region="us-east-1"),
    thing_name=app.node.try_get_context("demo_thing_name") or "shadow-talk-demo",
)
app.synth()
