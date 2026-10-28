from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_iot as iot,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as event_sources,
    aws_logs as logs,
)
from constructs import Construct


class ShadowTalkStack(Stack):
    """Three isolated comparisons: reads, named-shadow migration, and routing."""

    def __init__(self, scope: Construct, construct_id: str, *, thing_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        iot.CfnThing(self, "DemoThing", thing_name=thing_name)
        migration_thing_name = f"{thing_name}-migrate"
        iot.CfnThing(self, "MigrationThing", thing_name=migration_thing_name)
        cache = dynamodb.Table(
            self, "FieldCache",
            partition_key=dynamodb.Attribute(name="device_id", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            removal_policy=RemovalPolicy.DESTROY,
        )
        stream = kinesis.Stream(
            self, "ProjectedEvents", shard_count=1,
            retention_period=Duration.hours(24), removal_policy=RemovalPolicy.DESTROY,
        )
        code = lambda_.Code.from_asset("lambdas", exclude=["**/__pycache__/**", "**/*.pyc"])

        def function(name: str, handler: str, environment: dict[str, str]) -> lambda_.Function:
            log_group = logs.LogGroup(
                self, f"{name}Logs", retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            )
            return lambda_.Function(
                self, name,
                runtime=lambda_.Runtime.PYTHON_3_12,
                handler=handler,
                code=code,
                timeout=Duration.seconds(30),
                memory_size=256,
                log_group=log_group,
                environment=environment,
            )

        before_reader = function("BeforeReader", "reader.handler", {"PATTERN": "Before", "THING_NAME": thing_name})
        after_reader = function("AfterReader", "reader.handler", {
            "PATTERN": "After", "THING_NAME": thing_name, "CACHE_TABLE": cache.table_name,
        })
        cache.grant_read_write_data(after_reader)
        shadow_arn = self.format_arn(service="iot", resource="thing", resource_name=thing_name)
        for reader in (before_reader, after_reader):
            reader.add_to_role_policy(iam.PolicyStatement(actions=["iot:GetThingShadow"], resources=[shadow_arn]))
            reader.add_to_role_policy(iam.PolicyStatement(actions=["iot:DescribeEndpoint"], resources=["*"]))

        migration_lab = function("MigrationLab", "migration_lab.handler", {
            "MIGRATION_THING_NAME": migration_thing_name,
        })
        migration_arn = self.format_arn(service="iot", resource="thing", resource_name=migration_thing_name)
        # Named shadows authorize against thing/<name>/<shadowName>, so the classic ARN alone is not enough.
        named_shadow_arn = self.format_arn(service="iot", resource="thing", resource_name=f"{migration_thing_name}/*")
        migration_lab.add_to_role_policy(iam.PolicyStatement(
            actions=["iot:GetThingShadow", "iot:UpdateThingShadow"], resources=[migration_arn, named_shadow_arn],
        ))
        migration_lab.add_to_role_policy(iam.PolicyStatement(actions=["iot:DescribeEndpoint"], resources=["*"]))

        before_consumer = function("BeforeRouteConsumer", "route_consumer.handler", {"PATTERN": "Before"})
        after_consumer = function("AfterRouteConsumer", "route_consumer.handler", {
            "PATTERN": "After", "CACHE_TABLE": cache.table_name,
        })
        cache.grant_write_data(after_consumer)
        after_consumer.add_event_source(event_sources.KinesisEventSource(
            stream, starting_position=lambda_.StartingPosition.LATEST, batch_size=10,
            retry_attempts=2,
        ))

        documents_topic = f"$aws/things/{thing_name}/shadow/update/documents"
        full_topic = f"shadow-talk/{thing_name}/full"
        republish_role = iam.Role(self, "RepublishRole", assumed_by=iam.ServicePrincipal("iot.amazonaws.com"))
        republish_role.add_to_policy(iam.PolicyStatement(
            actions=["iot:Publish"],
            resources=[self.format_arn(service="iot", resource="topic", resource_name=full_topic)],
        ))
        full_rule = iot.CfnTopicRule(
            self, "FullDocumentRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql=f"SELECT * FROM '{documents_topic}'",
                aws_iot_sql_version="2016-03-23",
                actions=[iot.CfnTopicRule.ActionProperty(
                    republish=iot.CfnTopicRule.RepublishActionProperty(
                        role_arn=republish_role.role_arn, topic=full_topic,
                    ),
                )],
            ),
        )
        before_rule = iot.CfnTopicRule(
            self, "FullDocumentConsumerRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql=f"SELECT * FROM '{full_topic}'",
                aws_iot_sql_version="2016-03-23",
                actions=[iot.CfnTopicRule.ActionProperty(
                    lambda_=iot.CfnTopicRule.LambdaActionProperty(function_arn=before_consumer.function_arn),
                )],
            ),
        )
        before_consumer.add_permission(
            "AllowIoTRule", principal=iam.ServicePrincipal("iot.amazonaws.com"), source_arn=before_rule.attr_arn,
        )

        kinesis_role = iam.Role(self, "KinesisRuleRole", assumed_by=iam.ServicePrincipal("iot.amazonaws.com"))
        stream.grant_write(kinesis_role)
        projection_sql = (
            "SELECT topic(3) AS device_id, "
            "current.version AS version, "
            "current.state.reported.sku AS sku, "
            "current.state.reported.soldAsSku AS soldAsSku, "
            "current.state.reported.bin AS current_bin, "
            "previous.state.reported.bin AS previous_bin "
            f"FROM '{documents_topic}'"
        )
        iot.CfnTopicRule(
            self, "ProjectedKinesisRule",
            topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                sql=projection_sql,
                aws_iot_sql_version="2016-03-23",
                actions=[iot.CfnTopicRule.ActionProperty(
                    kinesis=iot.CfnTopicRule.KinesisActionProperty(
                        role_arn=kinesis_role.role_arn,
                        stream_name=stream.stream_name,
                        partition_key=thing_name,
                    ),
                )],
            ),
        )

        dashboard = cloudwatch.Dashboard(self, "DemoDashboard", dashboard_name="ShadowTalkDemo")
        def metric(name: str, pattern: str, *, statistic: str = "Sum", unit=None):
            return cloudwatch.Metric(
                namespace="ShadowTalkDemo", metric_name=name,
                dimensions_map={"Pattern": pattern},
                statistic=statistic, period=Duration.minutes(1), unit=unit,
            )

        dashboard.add_widgets(cloudwatch.TextWidget(markdown=(
            "# Death by a Thousand Shadows — live comparison\n"
            "Run `python scripts/demo.py` after deployment. Pattern 1: fewer reads. "
            "Pattern 2: classic-to-named migration and smaller touched records. "
            "Pattern 3: smaller routed events. Billing-unit metrics are **estimates**, not AWS charges. "
            "The After route targets Kinesis; its 5 KB message-unit series is a what-if republish comparison."
        ), width=24, height=3))
        dashboard.add_widgets(
            cloudwatch.GraphWidget(title="Shadow API calls per minute", left=[metric("ShadowGets", p) for p in ("Before", "After")], width=12),
            cloudwatch.GraphWidget(title="Reader duration p95 (ms)", left=[metric("InvocationMs", p, statistic="p95") for p in ("Before", "After")], width=12),
        )
        dashboard.add_widgets(
            cloudwatch.GraphWidget(title="Pattern 2: touched shadow record bytes", left=[metric("TouchedRecordBytes", p, statistic="Average") for p in ("Before", "After")], width=12),
            cloudwatch.GraphWidget(title="Pattern 2: units for one update (estimate)", left=[metric("TouchedUnitsEstimated", p, statistic="Average") for p in ("Before", "After")], width=12),
        )
        dashboard.add_widgets(
            cloudwatch.GraphWidget(title="Pattern 2: known route vs lookup units", left=[
                metric("KnownRouteUnitsEstimated", "Before", statistic="Average"),
                metric("KnownRouteUnitsEstimated", "After", statistic="Average"),
                metric("LookupRouteUnitsEstimated", "After", statistic="Average"),
            ], width=12),
            cloudwatch.GraphWidget(title="Pattern 2: migration and dual-write operations", left=[metric(m, "Migration") for m in ("MigrationGets", "MigrationUpdates", "DualWriteUpdates", "MigrationPaused")], width=12),
        )
        dashboard.add_widgets(
            cloudwatch.GraphWidget(title="Where After reads came from", left=[metric(m, "After") for m in ("PayloadHits", "ObjectHits", "CacheHits", "CacheMisses")], width=12),
            cloudwatch.GraphWidget(title="Estimated shadow operation units", left=[metric("ShadowUnitsEstimated", p) for p in ("Before", "After")], width=12),
        )
        dashboard.add_widgets(
            cloudwatch.GraphWidget(title="Observed route payload bytes", left=[metric("RoutedBytes", p) for p in ("Before", "After")], width=12),
            cloudwatch.GraphWidget(title="5 KB MQTT units if republished (estimate)", left=[metric("MqttUnitsIfRepublished", p) for p in ("Before", "After")], width=12),
        )
        dashboard.add_widgets(cloudwatch.GraphWidget(
            title="DynamoDB operations in After read path", left=[metric(m, "After") for m in ("CacheReads", "CacheWrites", "CacheErrors")], width=12,
        ))

        # Experiment 2 benchmark: scripts/named_shadow_benchmark.py --mode live --publish-cloudwatch.
        # Scenario names are user-defined, so SEARCH discovers series instead of hard-coding presets.
        def benchmark_search(metric_name: str, *, per_architecture: bool, with_metric_name: bool) -> cloudwatch.MathExpression:
            schema = "{ShadowTalkDemo,Architecture,Scenario}" if per_architecture else "{ShadowTalkDemo,Scenario}"
            # Dynamic labels apply per discovered series, e.g. "hot-settings classic".
            label = "${PROP('Dim.Scenario')}"
            if per_architecture:
                label += " ${PROP('Dim.Architecture')}"
            if with_metric_name:
                label += " ${PROP('MetricName')}"
            return cloudwatch.MathExpression(
                expression=f"SEARCH('{schema} MetricName=\"{metric_name}\"', 'Average', 300)",
                using_metrics={}, label=label, period=Duration.minutes(5),
            )

        def benchmark_bar(title: str, *metric_names: str, per_architecture: bool = True,
                          annotations: list[cloudwatch.HorizontalAnnotation] | None = None) -> cloudwatch.GraphWidget:
            return cloudwatch.GraphWidget(
                title=title, view=cloudwatch.GraphWidgetView.BAR, width=8, height=6,
                left=[benchmark_search(name, per_architecture=per_architecture, with_metric_name=len(metric_names) > 1)
                      for name in metric_names],
                left_annotations=annotations,
                # Bar views clamp the axis at zero, so every charted benchmark metric is nonnegative.
                left_y_axis=cloudwatch.YAxisProps(show_units=False),
            )

        dashboard.add_widgets(cloudwatch.TextWidget(markdown=(
            "## Experiment 2 benchmark: should this workload move to named shadows?\n"
            "Populated by `python scripts/named_shadow_benchmark.py --mode live --publish-cloudwatch`. "
            "Each bar is one scenario from the run's configuration; classic and named series are the same "
            "logical workload replayed on each architecture. All scenarios of a run share one timestamp, and bar views "
            "render the **earliest 5-minute bucket in the selected time range**, so set the range to start after any earlier run. "
            "Units are 1 KiB shadow-record estimates per logical business action, not AWS charges. "
            "In the ratio chart, 1.0 is break-even: below it the named design uses fewer units, above it more. "
            "Payback bars are absent when the scenario never pays back."
        ), width=24, height=3))
        dashboard.add_widgets(
            benchmark_bar("Exp 2: estimated units per logical action", "BenchmarkUnitsPerAction"),
            benchmark_bar("Exp 2: shadow API calls per logical action", "BenchmarkApiCallsPerAction"),
            benchmark_bar("Exp 2: action latency p95 (ms, sequential SDK calls)", "BenchmarkActionApiMsP95"),
        )
        dashboard.add_widgets(
            benchmark_bar("Exp 2: named / classic units per action (1.0 = break-even)", "BenchmarkNamedToClassicUnitRatio",
                          per_architecture=False, annotations=[cloudwatch.HorizontalAnnotation(
                              value=1.0, label="break-even", color=cloudwatch.Color.GREY)]),
            benchmark_bar("Exp 2: payback in logical actions per device", "BenchmarkPaybackActions", per_architecture=False),
            benchmark_bar("Exp 2: one-time migration and extra dual-write units per device",
                          "BenchmarkMigrationUnits", "BenchmarkOverlapExtraUnits", per_architecture=False),
        )
        dashboard.add_widgets(
            benchmark_bar("Exp 2: stored record bytes before (classic)", "BenchmarkClassicRecordBytes", per_architecture=False),
            benchmark_bar("Exp 2: stored record bytes after (named partitions + routing stub)", "BenchmarkNamedRecordBytes", per_architecture=False),
            benchmark_bar("Exp 2: response bytes per logical action", "BenchmarkResponseBytesPerAction"),
        )

        CfnOutput(self, "ThingName", value=thing_name)
        CfnOutput(self, "BeforeReaderName", value=before_reader.function_name)
        CfnOutput(self, "AfterReaderName", value=after_reader.function_name)
        CfnOutput(self, "MigrationThingName", value=migration_thing_name)
        CfnOutput(self, "MigrationFunctionName", value=migration_lab.function_name)
        CfnOutput(self, "CacheTableName", value=cache.table_name)
        CfnOutput(self, "DashboardName", value=dashboard.dashboard_name)
        CfnOutput(self, "FullDocumentRuleName", value=full_rule.ref)
