```mermaid
flowchart LR
  %% Operations
  sc([SOURCE CREATION])
  gs([GATHER STAGE])
  fs([FETCH STAGE])
  is([IMPORT STAGE])

  %% Conditions
  manual{Manual Trigger}
  scheduled{Scheduled Trigger}

  sc --> manual
  sc --> scheduled
  manual --> gs
  scheduled --> gs
  gs --> fs
  fs --> is
```
