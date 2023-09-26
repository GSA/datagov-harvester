# Pipeline Queue Infrastructure

-   Status: Accepted
-   Deciders: Tyler Burton
-   Date: 2023-07-21
-   Tags: infrastructure, brokerpaks, cloud-foundry

Technical Story: [TICKET]

## Context and Problem Statement

In order to orchestrate a pipeline, we needed to decide on an application methodology for queuing.

## Decision Drivers <!-- optional -->

-

## Considerations

There were several proposals for technologies:

-   Redis on Cloud.gov ([spike](https://github.com/GSA/data.gov/issues/4215))
-   Lambda & SQS on AWS([spike](https://github.com/GSA/data.gov/issues/4216))
-   AWS Glue
-   Some hybridization of the above tech\

## Decision

Chosen option: Lambda & SQS on AWS

### Positive Consequences <!-- optional -->

### Negative Consequences <!-- optional -->

## Pros and Cons of the Options

### Redis on Cloud.gov

Pros:

-   Lightweight
-   Uses technologies we already know
-   No development of services needed
-   Allows full insight/control of the queue

Cons:

-   Self-managed queue (we can implement things wrong and break stuff)
-   Scaling up may be cost prohibitive
-   No auto scaling

### Lambda & SQS on AWS

Pros:

-   Industry standard
-   Proven and effective
-   UI and various tools to track and debug
-   Auto scaling/speed

Cons:

-   Have to create the brokerpak, and maintain it long term.
-   May be more difficult to make queue/status publicly available
-   Possibly could break the 15 minute max with WAF extract\*\*

### AWS Glue

Still quite a bit unknown here. It would be a managed service, but the cost and if the tool makes sense for our workflow (extract, compare, transform, validate, load [add/update/delete]) is up in the air. We would probably be trading off flexibility for well defined process. See future ticket [here](https://github.com/GSA/data.gov/issues/4279).

### Hybrid path

This would seek to mitigate the cons in both the AWS world (SQS + Lambda) and the Cloud.gov world (Redis + Celery) with the best of both worlds. Namely:
Cloud.gov: would have the extract and compare processes/pipelines. These are very different from the rest, in that they need context, they need to be fully complete before the next pieces can start, and they require more flexibility in usage (working with the outside world)
AWS: would have the transformation, validation, and load (add/update/delete) pipelines. These are agnostic, and perfect for the AWS queue system. We don’t need to wait, we don’t need context. It just needs to do the job and report on its success/failure.

Pros

-   Auto-scaling where we need it (record level)
-   Allows most control of troublesome processes (extract, compare)
-   Allows full control for “kicking off” job

Cons:

-   Have to create the buildpack, and maintain it long term (though the options needed to build in should be more limited)
-   Have to have knowledge of celery (custom queue) and AWS SQS

## Links <!-- optional -->
