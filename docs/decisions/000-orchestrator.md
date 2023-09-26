# Orchestration

- Status: Accepted
- Impact: High
- Driver: Tyler Burton
- Approver: Hyon Kim
- Contributors: Datagov Team
- Informed: TTS Leadership
- Date: 2023-09-26
- Tags: infrastructure, orchestration
- Outcome: Airflow

Technical Story: [TICKET](TBD link to implement basic Airflow Docker install)

## Context and Problem Statement

In order to construct an ELT pipeline, an orchestration framework must be chosen or developed.

## Decision Drivers <!-- optional -->

- Solution must be FEDRamped, or the SSB needs to be extended to support it.

## Considerations

- ETL Pipeline with Python Celery ([spike](https://github.com/GSA/data.gov/issues/4215))
- Lambda & SQS on AWS([spike](https://github.com/GSA/data.gov/issues/4216))
- AWS Glue([spike](https://github.com/GSA/data.gov/issues/4279))
- Airflow([spike1](https://github.com/GSA/data.gov/issues/4422), [spike2](https://github.com/GSA/data.gov/issues/4434))

## Pros and Cons of the Options

### Celery on Cloud.gov

Pros:

- Lightweight
- Uses technologies we already know
- No development of services needed
- Allows full insight/control of the queue

Cons:

- Self-managed
- Scaling up may be cost prohibitive
- No benefit of community ecosystem

### Lambda & SQS on AWS

Pros:

- Industry standard
- Proven and effective
- UI and various tools to track and debug
- Auto scaling/speed
- Granular cost
- Fully Distributed System

Cons:

- Not FEDRamped. Must be brokered.
- Limited observability into system due to cloud.gov restrictions
- Possibly could break the 15 minute max for with WAF extract**

### AWS Glue

Pros: 
 - Managed service.
 - All in one data integration service
 - Might be enough for what we need

Cons:
 - [Must use Spark for transforms](https://www.nexla.com/data-engineering-best-practices/glue-vs-airflow/)
 - Vendor lockin
 - Unsure extensability
 
### Airflow

Pros:

- Off the shelf orchestration solution
- Open source
- Active community
- Ability to iterate towards a mature solution while launched

Cons:

- Not FEDRamped. Must be brokered.

## Decision

Chosen option: Airflow

### Positive Consequences <!-- optional -->

### Negative Consequences <!-- optional -->

## Links <!-- optional -->
