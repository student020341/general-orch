# System Architecture Documentation: state convergence orchestration

> Auto-generated system ontology, type specifications, and action signatures for technical documentation and AI ingestion.

## Registered Scalar Types

- `text`
- `number`
- `any`

## Types & Components

### `orchestrator`

- **Namespace**: `orchestrator`
- **Type Name**: orchestrator
- **Parent**: *(Root Level)*

#### Description
the state convergence orchestrator, can diff current state and target state and attempt to align/converge on the target

#### Properties
*No custom properties defined.*

#### Child Types / Sub-Components
- `orchestrator.state manager`
- `orchestrator.event stream`
- `orchestrator.workflow`

#### Actions & Methods
##### `update(target: target system, args: any)`

- **Target**: `target system`
- **Description**: some kind of side effect or configuration for another target, known or unknown to the external system
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `target system` | Destination | Target receiving entity |
  | `args` | `any` | Action Property | Parameter for this action |

#### Inbound Actions
- From `external system`: `send configuration`

---

### `orchestrator.state manager`

- **Namespace**: `orchestrator.state manager`
- **Type Name**: state manager
- **Parent**: `orchestrator`

#### Description
system that subscribes to event stream, looks up or creates records for the target entity, diffs and updates the state, creates an execution plan, and dispatches to nats/temporal

#### Properties
*No custom properties defined.*

#### Child Types / Sub-Components
- `orchestrator.state manager.store`
- `orchestrator.state manager.reconciler`
- `orchestrator.state manager.dispatcher`
- `orchestrator.state manager.entry point`
- `orchestrator.state manager.exit point`

#### Actions & Methods
##### `response event(target: orchestrator.event stream)`

- **Target**: `orchestrator.event stream`
- **Description**: *No description provided.*
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.event stream` | Destination | Target receiving entity |

#### Inbound Actions
- From `orchestrator.event stream`: `push input event`

---

### `orchestrator.state manager.store`

- **Namespace**: `orchestrator.state manager.store`
- **Type Name**: store
- **Parent**: `orchestrator.state manager`

#### Description
abstract storage that contains current state and target/input state. also track things like status and  version ids to help coordinate async systems

#### Properties
| Property | Scalar Type |
| :--- | :--- |
| `current` | `any` |
| `target` | `any` |
| `status` | `text` |
| `version` | `text` |

#### Actions & Methods
*No outgoing actions defined.*

#### Inbound Actions
- From `orchestrator.state manager.reconciler`: `read states`
- From `orchestrator.state manager.dispatcher`: `update progress`
- From `orchestrator.state manager.dispatcher`: `wirte plan steps`

---

### `orchestrator.state manager.reconciler`

- **Namespace**: `orchestrator.state manager.reconciler`
- **Type Name**: reconciler
- **Parent**: `orchestrator.state manager`

#### Description
reads states, diffs, builds execution plan to reconcile diff

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `read states(target: orchestrator.state manager.store)`

- **Target**: `orchestrator.state manager.store`
- **Description**: *No description provided.*
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager.store` | Destination | Target receiving entity |

##### `emit plan(target: orchestrator.state manager.dispatcher)`

- **Target**: `orchestrator.state manager.dispatcher`
- **Description**: a DAG plan that has steps like { task_queue: "camera-workers", activity: "SetIP/ConfigureDevice", payload: { vendor: "canon" , ip: 1234} }, then a temporal flow for camera-workers will create the appropriate driver for the given vendor. The reconciler maps the input to a domain queue and capability.
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager.dispatcher` | Destination | Target receiving entity |

#### Inbound Actions
- From `orchestrator.state manager.entry point`: `start`

---

### `orchestrator.state manager.dispatcher`

- **Namespace**: `orchestrator.state manager.dispatcher`
- **Type Name**: dispatcher
- **Parent**: `orchestrator.state manager`

#### Description
updates status of entity in store and dispatches to nats/temporal

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `update progress(target: orchestrator.state manager.store)`

- **Target**: `orchestrator.state manager.store`
- **Description**: *No description provided.*
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager.store` | Destination | Target receiving entity |

##### `end(target: orchestrator.state manager.exit point)`

- **Target**: `orchestrator.state manager.exit point`
- **Description**: dispatcher sends a plan/activity to a temporal workflow
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager.exit point` | Destination | Target receiving entity |

##### `wirte plan steps(target: orchestrator.state manager.store)`

- **Target**: `orchestrator.state manager.store`
- **Description**: the reconciler creates a dag of execution steps, dispatcher will persist to the store, and the temporal workflow will read the steps
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager.store` | Destination | Target receiving entity |

#### Inbound Actions
- From `orchestrator.state manager.reconciler`: `emit plan`

---

### `orchestrator.state manager.entry point`

- **Namespace**: `orchestrator.state manager.entry point`
- **Type Name**: entry point
- **Parent**: `orchestrator.state manager`

#### Description
not a real class, just an entry point for the diagram

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `start(target: orchestrator.state manager.reconciler)`

- **Target**: `orchestrator.state manager.reconciler`
- **Description**: state manager sets target config in store and engages reconciler. This payload will be something like { subject, target } where subject is something like camera.canon that can be parsed into a domain (cameras) and brand (canon)
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager.reconciler` | Destination | Target receiving entity |

---

### `orchestrator.state manager.exit point`

- **Namespace**: `orchestrator.state manager.exit point`
- **Type Name**: exit point
- **Parent**: `orchestrator.state manager`

#### Description
not a real class, just an exit point for the diagram

#### Properties
*No custom properties defined.*

#### Actions & Methods
*No outgoing actions defined.*

#### Inbound Actions
- From `orchestrator.state manager.dispatcher`: `end`

---

### `orchestrator.event stream`

- **Namespace**: `orchestrator.event stream`
- **Type Name**: event stream
- **Parent**: `orchestrator`

#### Description
this node consumes and emits events

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `push input event(target: orchestrator.state manager, entity: text, state: any)`

- **Target**: `orchestrator.state manager`
- **Description**: notify state manager of new desired state for some entity
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.state manager` | Destination | Target receiving entity |
  | `entity` | `text` | Action Property | Parameter for this action |
  | `state` | `any` | Action Property | Parameter for this action |

##### `push action event(target: orchestrator.workflow, task queue: text, capability: text, payload: any, plan: text)`

- **Target**: `orchestrator.workflow`
- **Description**: event originating from reconciler/dispatcher that will route to a specific temporal workflow (by task queue, like camera-workers), then capability + payload.vendor will get a driver to execute the action(s). Input might also be a plan to execute which will be a dag stored in the store.
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.workflow` | Destination | Target receiving entity |
  | `task queue` | `text` | Action Property | Parameter for this action |
  | `capability` | `text` | Action Property | Parameter for this action |
  | `payload` | `any` | Action Property | Parameter for this action |
  | `plan` | `text` | Action Property | Parameter for this action |

#### Inbound Actions
- From `orchestrator.state manager`: `response event`

---

### `orchestrator.workflow`

- **Namespace**: `orchestrator.workflow`
- **Type Name**: workflow
- **Parent**: `orchestrator`

#### Description
a temporal workflow, probably

#### Properties
*No custom properties defined.*

#### Child Types / Sub-Components
- `orchestrator.workflow.orchestration-workflow`
- `orchestrator.workflow.domain-worker`
- `orchestrator.workflow.store`
- `orchestrator.workflow.entry point`
- `orchestrator.workflow.exit point`

#### Actions & Methods
*No outgoing actions defined.*

#### Inbound Actions
- From `orchestrator.event stream`: `push action event`

---

### `orchestrator.workflow.orchestration-workflow`

- **Namespace**: `orchestrator.workflow.orchestration-workflow`
- **Type Name**: orchestration-workflow
- **Parent**: `orchestrator.workflow`

#### Description
reads plan from store, sorts dag to determine steps that are ready to run, dispatches plans to their domain specific workers, ex send task to camera-worker to set camera public ip

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `read plan(target: orchestrator.workflow.store)`

- **Target**: `orchestrator.workflow.store`
- **Description**: *No description provided.*
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.workflow.store` | Destination | Target receiving entity |

##### `dispatch(target: orchestrator.workflow.domain-worker)`

- **Target**: `orchestrator.workflow.domain-worker`
- **Description**: example:  domain worker deals with camera config, request to configure camera ip
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.workflow.domain-worker` | Destination | Target receiving entity |

##### `end(target: orchestrator.workflow.exit point)`

- **Target**: `orchestrator.workflow.exit point`
- **Description**: orchestration workflow emits status updates to nats that another component will read and update the store status
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.workflow.exit point` | Destination | Target receiving entity |

#### Inbound Actions
- From `orchestrator.workflow.entry point`: `start`
- From `orchestrator.workflow.domain-worker`: `result`

---

### `orchestrator.workflow.domain-worker`

- **Namespace**: `orchestrator.workflow.domain-worker`
- **Type Name**: domain-worker
- **Parent**: `orchestrator.workflow`

#### Description
*No description provided.*

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `result(target: orchestrator.workflow.orchestration-workflow)`

- **Target**: `orchestrator.workflow.orchestration-workflow`
- **Description**: worker returns result to orchestrator, orchestrator will publish result to event stream
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.workflow.orchestration-workflow` | Destination | Target receiving entity |

#### Inbound Actions
- From `orchestrator.workflow.orchestration-workflow`: `dispatch`

---

### `orchestrator.workflow.store`

- **Namespace**: `orchestrator.workflow.store`
- **Type Name**: store
- **Parent**: `orchestrator.workflow`

#### Description
*No description provided.*

#### Properties
*No custom properties defined.*

#### Actions & Methods
*No outgoing actions defined.*

#### Inbound Actions
- From `orchestrator.workflow.orchestration-workflow`: `read plan`

---

### `orchestrator.workflow.entry point`

- **Namespace**: `orchestrator.workflow.entry point`
- **Type Name**: entry point
- **Parent**: `orchestrator.workflow`

#### Description
*No description provided.*

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `start(target: orchestrator.workflow.orchestration-workflow)`

- **Target**: `orchestrator.workflow.orchestration-workflow`
- **Description**: *No description provided.*
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator.workflow.orchestration-workflow` | Destination | Target receiving entity |

---

### `orchestrator.workflow.exit point`

- **Namespace**: `orchestrator.workflow.exit point`
- **Type Name**: exit point
- **Parent**: `orchestrator.workflow`

#### Description
*No description provided.*

#### Properties
*No custom properties defined.*

#### Actions & Methods
*No outgoing actions defined.*

#### Inbound Actions
- From `orchestrator.workflow.orchestration-workflow`: `end`

---

### `external system`

- **Namespace**: `external system`
- **Type Name**: external system
- **Parent**: *(Root Level)*

#### Description
*No description provided.*

#### Properties
*No custom properties defined.*

#### Actions & Methods
##### `send configuration(target: orchestrator, state: any)`

- **Target**: `orchestrator`
- **Description**: *No description provided.*
- **Parameters**:
  | Parameter | Type | Source | Description / Notes |
  | :--- | :--- | :--- | :--- |
  | `target` | `orchestrator` | Destination | Target receiving entity |
  | `state` | `any` | Action Property | Parameter for this action |

---

### `target system`

- **Namespace**: `target system`
- **Type Name**: target system
- **Parent**: *(Root Level)*

#### Description
Maybe hardware updated via ssh, maybe an object in a third party api, maybe an object in a local database, maybe an object in a first party (owned by us) micro service

#### Properties
*No custom properties defined.*

#### Actions & Methods
*No outgoing actions defined.*

#### Inbound Actions
- From `orchestrator`: `update`

---

### `todo`

- **Namespace**: `todo`
- **Type Name**: todo
- **Parent**: *(Root Level)*

#### Description
point raised by ai review of export - the entry/exit point nodes are unnecessary if cross layer actions are supported, replace them

#### Properties
*No custom properties defined.*

#### Actions & Methods
*No outgoing actions defined.*

---

## System Actions Directory

Complete listing of all actions connecting entities across the system.

| Action / Method | Source | Target | Parameters | Description |
| :--- | :--- | :--- | :--- | :--- |
| `send configuration` | `external system` | `orchestrator` | `state: any` | - |
| `update` | `orchestrator` | `target system` | `args: any` | some kind of side effect or configuration for another target, known or unknown to the external system |
| `push input event` | `orchestrator.event stream` | `orchestrator.state manager` | `entity: text, state: any` | notify state manager of new desired state for some entity |
| `response event` | `orchestrator.state manager` | `orchestrator.event stream` | `*(none)*` | - |
| `read states` | `orchestrator.state manager.reconciler` | `orchestrator.state manager.store` | `*(none)*` | - |
| `emit plan` | `orchestrator.state manager.reconciler` | `orchestrator.state manager.dispatcher` | `*(none)*` | a DAG plan that has steps like { task_queue: "camera-workers", activity: "SetIP/ConfigureDevice", payload: { vendor: "canon" , ip: 1234} }, then a temporal flow for camera-workers will create the appropriate driver for the given vendor. The reconciler maps the input to a domain queue and capability. |
| `update progress` | `orchestrator.state manager.dispatcher` | `orchestrator.state manager.store` | `*(none)*` | - |
| `start` | `orchestrator.state manager.entry point` | `orchestrator.state manager.reconciler` | `*(none)*` | state manager sets target config in store and engages reconciler. This payload will be something like { subject, target } where subject is something like camera.canon that can be parsed into a domain (cameras) and brand (canon) |
| `end` | `orchestrator.state manager.dispatcher` | `orchestrator.state manager.exit point` | `*(none)*` | dispatcher sends a plan/activity to a temporal workflow |
| `wirte plan steps` | `orchestrator.state manager.dispatcher` | `orchestrator.state manager.store` | `*(none)*` | the reconciler creates a dag of execution steps, dispatcher will persist to the store, and the temporal workflow will read the steps |
| `push action event` | `orchestrator.event stream` | `orchestrator.workflow` | `task queue: text, capability: text, payload: any, plan: text` | event originating from reconciler/dispatcher that will route to a specific temporal workflow (by task queue, like camera-workers), then capability + payload.vendor will get a driver to execute the action(s). Input might also be a plan to execute which will be a dag stored in the store. |
| `read plan` | `orchestrator.workflow.orchestration-workflow` | `orchestrator.workflow.store` | `*(none)*` | - |
| `start` | `orchestrator.workflow.entry point` | `orchestrator.workflow.orchestration-workflow` | `*(none)*` | - |
| `dispatch` | `orchestrator.workflow.orchestration-workflow` | `orchestrator.workflow.domain-worker` | `*(none)*` | example:  domain worker deals with camera config, request to configure camera ip |
| `result` | `orchestrator.workflow.domain-worker` | `orchestrator.workflow.orchestration-workflow` | `*(none)*` | worker returns result to orchestrator, orchestrator will publish result to event stream |
| `end` | `orchestrator.workflow.orchestration-workflow` | `orchestrator.workflow.exit point` | `*(none)*` | orchestration workflow emits status updates to nats that another component will read and update the store status |
