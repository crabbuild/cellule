export const walkthroughs = [
  {
    id: "sql",
    name: "An order",
    category: "SQL",
    title: "Save an order. Read what committed.",
    introduction:
      "Follow order 42 from a typed command to a receipt-bound query. The runnable example uses one fixed-shard Orders Cell and verifies a total of 1999 cents.",
    output: "order 42 total: 1999 cents",
    guide: "/docs/primitives/sql",
    invariant:
      "The state change and its request outcome share one SQLite transaction. Success waits for the durability gate.",
    steps: [
      {
        label: "Address",
        title: "Choose the Cell and request identity.",
        description:
          "The application obtains its typed SQL handle, prepares a parameterized INSERT, and supplies a stable identity for this logical mutation.",
        state: "Command prepared",
        detail: "Order 42 · total 1999 cents",
        observation: "No successful outcome returned yet.",
      },
      {
        label: "Commit",
        title: "Record the change and the outcome together.",
        description:
          "Managed SQLite commits the order row and its request outcome in one transaction. This local commit is part of the path; it is not yet the promise made by a successful command response.",
        state: "Local transaction committed",
        detail: "Order row + request outcome",
        observation: "The response gate remains closed.",
      },
      {
        label: "Publish",
        title: "Establish recoverable durability.",
        description:
          "On the object-publication path, verified LTX bytes are stored and the exact root is pinned through fenced authority. The successful result includes a receipt.",
        state: "Durable outcome published",
        detail: "Result + committed receipt",
        observation: "The outcome can now back a successful reply.",
      },
      {
        label: "Observe",
        title: "Read at the returned receipt.",
        description:
          "The application queries with the receipt and checks the actual order value. The read must observe that committed position, rather than merely succeeding as a transport operation.",
        state: "Receipt-bound read verified",
        detail: "order 42 · 1999 cents",
        observation: "The example verifies the saved value, then drains.",
      },
    ],
  },
  {
    id: "blob",
    name: "An upload",
    category: "BLOB",
    title: "Stage content. Publish its reference.",
    introduction:
      "Follow a small receipt attachment through a multipart upload. The distinction to watch is stored part bytes versus a completed, visible Blob reference.",
    output: "attachment stored: receipt for order 42",
    guide: "/docs/primitives/blob",
    invariant:
      "Staging a part does not make the Blob visible. Complete publishes the reference in the Cell’s durable state.",
    steps: [
      {
        label: "Begin",
        title: "Open the upload.",
        description:
          "The application begins an upload for orders/42/receipt.txt. Each logical mutation has its own request identity; retrying that mutation preserves its identity.",
        state: "Upload opened",
        detail: "Blob visibility: hidden",
        observation: "A started upload is not a completed attachment.",
      },
      {
        label: "Stage",
        title: "Store an immutable part.",
        description:
          "PutPart stages the content bytes in object storage. Those bytes exist, but the application has not yet published the completed Blob reference.",
        state: "Part bytes staged",
        detail: "Blob visibility: hidden",
        observation: "Stored bytes alone do not establish visible content.",
      },
      {
        label: "Complete",
        title: "Publish the completed reference.",
        description:
          "Complete commits the Blob reference through the Cell’s durable mutation path. Its result carries the completion receipt.",
        state: "Completed reference published",
        detail: "Blob visibility: visible",
        observation: "Use the completion receipt for the subsequent read.",
      },
      {
        label: "Read",
        title: "Verify the attachment body.",
        description:
          "The example reads at the completion receipt and checks the returned bytes against receipt for order 42 before draining the local runtime.",
        state: "Content bytes verified",
        detail: "receipt for order 42",
        observation: "The full path verifies both publication and content.",
      },
    ],
  },
  {
    id: "workflow",
    name: "A workflow",
    category: "WORKFLOW + ACTIVITIES",
    title: "Remember decisions. Supervise the work.",
    introduction:
      "Follow the welcome workflow and its Echo Activity. The example echoes local bytes; a real external integration belongs to your Activity handler.",
    output: "workflow welcome/42: completed",
    guide: "/docs/primitives/workflow",
    invariant:
      "Workflow decisions and intent are durable. External Activity execution happens outside SQLite and must be safe to repeat.",
    steps: [
      {
        label: "Start",
        title: "Start the welcome run.",
        description:
          "The application starts the workflow with a pinned definition. Its transition decides what should happen next.",
        state: "Start command submitted",
        detail: "Workflow: welcome/42",
        observation: "The definition determines the next durable decision.",
      },
      {
        label: "Intent",
        title: "Record Running and Activity intent.",
        description:
          "The start transition records the workflow’s Running state and the request for an Echo Activity together in durable history.",
        state: "Running · waiting for Activity",
        detail: "Activity intent: echo",
        observation: "The decision is recorded before external work runs.",
      },
      {
        label: "Execute",
        title: "Claim, validate, and execute.",
        description:
          "ActivitySupervisor claims the work and validates the lease. The handler executes outside the SQLite transaction. Real service calls should use the stable Activity idempotency key.",
        state: "Activity executing",
        detail: "External work: local Echo handler",
        observation:
          "A lease does not make arbitrary external side effects exactly-once.",
      },
      {
        label: "Complete",
        title: "Record the completion.",
        description:
          "The supervisor records the Activity result. The workflow transition consumes that event and records Completed state.",
        state: "Completed outcome recorded",
        detail: "Activity result: welcome sent",
        observation:
          "Completion becomes part of the workflow’s durable history.",
      },
      {
        label: "Observe",
        title: "Read the completed run.",
        description:
          "The application reads at the returned receipt, verifies Completed, and drains the runtime and its supervised work.",
        state: "Completed state verified",
        detail: "Workflow: welcome/42",
        observation:
          "The example checks durable state, not just handler execution.",
      },
    ],
  },
  {
    id: "schedules",
    name: "A scheduled delivery",
    category: "CRON + EFFECTS",
    title: "From a due tick to one recorded reminder.",
    introduction:
      "Follow a recurring schedule through an explicitly driven maintenance tick and an Effect delivery to a destination Cell.",
    output: "schedule reminder: one occurrence delivered",
    guide: "/docs/primitives/effects",
    invariant:
      "The source intent is durable. The destination applies delivery idempotently in its own transaction; there is no shared transaction across both Cells.",
    steps: [
      {
        label: "Schedule",
        title: "Store the schedule.",
        description:
          "The application upserts a fixed-interval schedule and reads it at the returned receipt. Declaring a schedule does not start a background timer on its own.",
        state: "Schedule stored",
        detail: "Reminder rows: 0",
        observation:
          "The serving application owns the scanner and supervisors.",
      },
      {
        label: "Tick",
        title: "Record a due occurrence.",
        description:
          "An explicit maintenance tick advances the due schedule and records durable Effect intent for the occurrence.",
        state: "Effect intent recorded",
        detail: "Reminder rows: 0",
        observation: "Intent is committed before delivery is attempted.",
      },
      {
        label: "Deliver",
        title: "Supervise the delivery.",
        description:
          "EffectSupervisor delivers through a signed in-process peer loopback in this example. Real deployments supply their transport and authorization integration.",
        state: "Delivery in progress",
        detail: "Reminder rows: 0",
        observation: "Source and destination remain separate Cells.",
      },
      {
        label: "Apply",
        title: "Apply through the destination inbox.",
        description:
          "The receiver processes the delivery idempotently and records the reminder. The destination transaction is separate from the source schedule transaction.",
        state: "Destination applied the occurrence",
        detail: "Reminder rows: 1",
        observation:
          "Repeated delivery must not create repeated application effects.",
      },
      {
        label: "Observe",
        title: "Confirm one reminder.",
        description:
          "The example reads the destination and verifies one row for the schedule occurrence, then drains the local Cells.",
        state: "One reminder verified",
        detail: "Reminder rows: 1",
        observation:
          "The check follows the work all the way to the destination.",
      },
    ],
  },
] as const;
