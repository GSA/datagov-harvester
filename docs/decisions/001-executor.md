# Title

- Status: In Progress
- Impact: High
- Driver: Tyler Burton
- Approver: Hyon Kim
- Contributors: Datagov Team
- Informed: TTS Leadership
- Date: TBD
- Tags: executor, airflow
- Outcome: TBD

Technical Story: [TICKET]()

## Context and Problem Statement

In order to process work with the least amount of interruption, datagovteam requires a harvester that is scalable in order to handle spikes in traffic, and resilient enough to continue processing jobs when it encounters errors or load

## Decision Drivers

> NOTE: This decision might be mooted when Airflow releases support for conditional executors based on the task.

## Considerations

- LocalExecutor
- CeleryExecutor
- KubernetesExecutor

## Pros and Cons of the Options

### LocalExecutor

Pros:

- 

Cons:

- 

### CeleryExecutor

Pros:

- 

Cons:

- 

### KubernetesExecutor

Pros:

- 

Cons:

- 

## Decision


### Positive Consequences <!-- optional -->

### Negative Consequences <!-- optional -->

## Links

- https://dataanddevops.com/apache-airflow-which-executor-to-use-in-production
