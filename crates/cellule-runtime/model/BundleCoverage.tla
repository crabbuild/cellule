------------------------ MODULE BundleCoverage ------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC
CONSTANTS NodeOnly, SkipComplete, SkipPrefix, UnprovedRead, UnpinnedGC
Cells == {1, 2}
Bundles == {1, 2}
RowCells(b) == IF b = 1 THEN Cells ELSE {2}
VARIABLES open, version, head, epoch, closed, endpoint,
          stage, proposedVersion, proposedHead, proposedEpoch, complete,
          present, proof, ack, visible, root, badSelection, badPrefix,
          badContents
vars == <<open, version, head, epoch, closed, endpoint,
          stage, proposedVersion, proposedHead, proposedEpoch, complete,
          present, proof, ack, visible, root, badSelection, badPrefix,
          badContents>>
CellSelected(c) == IF stage[2] = 3 /\ c \in RowCells(2) THEN 2
                   ELSE IF stage[1] = 3 THEN 1 ELSE 0
Init == /\ open = TRUE /\ version = 0 /\ head = 0
        /\ epoch = [c \in Cells |-> 1] /\ closed = {}
        /\ endpoint = [c \in Cells |-> -1]
        /\ stage = [b \in Bundles |-> 0]
        /\ proposedVersion = [b \in Bundles |-> -1]
        /\ proposedHead = [b \in Bundles |-> -1]
        /\ proposedEpoch = [b \in Bundles |-> [c \in Cells |-> 0]]
        /\ complete = {} /\ present = {} /\ proof = {}
        /\ ack = [c \in Cells |-> 0] /\ visible = [c \in Cells |-> 0]
        /\ root = [c \in Cells |-> 0]
        /\ badSelection = FALSE /\ badPrefix = FALSE /\ badContents = FALSE

(* The first bundle carries both Cells; the second carries only the hot Cell.
   Each row carries an exact mutation AND retry outcome for its Cell.
   Complete abstracts authentication of all bytes, outcomes and dependencies.
   It is a checked input, not an assumption about every uploaded object. *)
Prepare(b) == /\ stage[b] = 0 /\ open /\ RowCells(b) \cap closed = {}
              /\ (SkipPrefix \/ b = head + 1)
              /\ stage' = [stage EXCEPT ![b] = 1]
              /\ proposedVersion' = [proposedVersion EXCEPT ![b] = version]
              /\ proposedHead' = [proposedHead EXCEPT ![b] = head]
              /\ proposedEpoch' = [proposedEpoch EXCEPT ![b] = epoch]
              /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                   complete, present, proof, ack, visible, root,
                   badSelection, badPrefix, badContents>>
Upload(b, good) == /\ stage[b] = 1
                  /\ stage' = [stage EXCEPT ![b] = 2]
                  /\ present' = present \cup {b}
                  /\ complete' = (IF good THEN complete \cup {b} ELSE complete)
                  /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                       proposedVersion, proposedHead, proposedEpoch, proof,
                       ack, visible, root, badSelection, badPrefix, badContents>>
Select(b) == /\ stage[b] = 2 /\ open /\ b \in present
             /\ (SkipComplete \/ b \in complete)
             /\ (NodeOnly \/ (proposedVersion[b] = version
                      /\ RowCells(b) \cap closed = {}
                      /\ \A c \in RowCells(b) : proposedEpoch[b][c] = epoch[c]))
             /\ (SkipPrefix \/ (proposedHead[b] = head /\ b = head + 1))
             /\ stage' = [stage EXCEPT ![b] = 3]
             /\ head' = b /\ version' = version + 1
             /\ badSelection' = (badSelection \/ RowCells(b) \cap closed # {}
                      \/ \E c \in RowCells(b) : proposedEpoch[b][c] # epoch[c])
             /\ badPrefix' = (badPrefix \/ b # head + 1 \/ proposedHead[b] # head)
             /\ badContents' = (badContents \/ b \notin complete)
             /\ UNCHANGED <<open, epoch, closed, endpoint, proposedVersion,
                  proposedHead, proposedEpoch, complete, present, proof,
                  ack, visible, root>>

(* A disjoint binding closure invalidates the old CAS, but need not strand the
   hot Cell. Reuse the immutable bytes only after fresh row-by-row authorization
   and predecessor verification; a closed participating binding cannot rebase. *)
Rebase(b) == /\ stage[b] = 2 /\ open /\ b \in present
             /\ proposedVersion[b] # version /\ b = head + 1
             /\ proposedHead[b] = head /\ RowCells(b) \cap closed = {}
             /\ (\A c \in RowCells(b) : proposedEpoch[b][c] = epoch[c])
             /\ proposedVersion' = [proposedVersion EXCEPT ![b] = version]
             /\ UNCHANGED <<open, version, head, epoch, closed, endpoint, stage,
                  proposedHead, proposedEpoch, complete, present, proof,
                  ack, visible, root, badSelection, badPrefix, badContents>>

(* A lost CAS reply leaves stage=3 and proof absent. Fresh exact reconciliation
   may mint the SAME selection; it cannot mint a newly uploaded candidate.
   A later selected head does not erase the earlier immutable selection. *)
Reconcile(b) == /\ stage[b] = 3 /\ b \notin proof
                /\ b \in present /\ (SkipComplete \/ b \in complete)
                /\ proof' = proof \cup {b}
                /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                     stage, proposedVersion, proposedHead, proposedEpoch,
                     complete, present, ack, visible, root,
                     badSelection, badPrefix, badContents>>
Ack(c, b) == /\ open /\ b \in proof /\ c \in RowCells(b) /\ c \notin closed
             /\ proposedEpoch[b][c] = epoch[c] /\ ack[c] < b
             /\ ack' = [ack EXCEPT ![c] = b]
             /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                  stage, proposedVersion, proposedHead, proposedEpoch,
                  complete, present, proof, visible, root,
                  badSelection, badPrefix, badContents>>
Read(c, b) == /\ open /\ visible[c] < b /\ (UnprovedRead \/ b \in proof)
              /\ c \in RowCells(b)
              /\ proposedEpoch[b][c] = epoch[c] /\ c \notin closed
              /\ visible' = [visible EXCEPT ![c] = b]
              /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                   stage, proposedVersion, proposedHead, proposedEpoch,
                   complete, present, proof, ack, root,
                   badSelection, badPrefix, badContents>>
(* Abstract a fully independent checkpoint selected by the authority catalog.
   A root descriptor still referencing this bundle is NOT such a checkpoint.
   Byte verification, dependency rewriting and complete reference inventory
   remain production obligations, not properties proved by this abstraction. *)
Materialize(c, b) == /\ b \in proof /\ b \in present /\ b \in complete
                    /\ c \in RowCells(b)
                    /\ root[c] < b
                    /\ root' = [root EXCEPT ![c] = b]
                    /\ version' = version + 1
                    /\ UNCHANGED <<open, head, epoch, closed, endpoint,
                         stage, proposedVersion, proposedHead, proposedEpoch,
                         complete, present, proof, ack, visible,
                         badSelection, badPrefix, badContents>>
Close(c) == /\ c \notin closed
            /\ closed' = closed \cup {c}
            /\ endpoint' = [endpoint EXCEPT ![c] = CellSelected(c)]
            /\ version' = version + 1
            /\ UNCHANGED <<open, head, epoch, stage, proposedVersion,
                 proposedHead, proposedEpoch, complete, present, proof,
                 ack, visible, root, badSelection, badPrefix, badContents>>
Transfer(c) == /\ c \in closed /\ epoch[c] = 1
               /\ root[c] >= endpoint[c]
               /\ epoch' = [epoch EXCEPT ![c] = 2]
               /\ UNCHANGED <<open, version, head, closed, endpoint,
                    stage, proposedVersion, proposedHead, proposedEpoch,
                    complete, present, proof, ack, visible, root,
                    badSelection, badPrefix, badContents>>
FenceNode == /\ open /\ open' = FALSE /\ version' = version + 1
             /\ UNCHANGED <<head, epoch, closed, endpoint, stage,
                  proposedVersion, proposedHead, proposedEpoch, complete,
                  present, proof, ack, visible, root,
                  badSelection, badPrefix, badContents>>
Collect(b) == /\ b \in present
              /\ (UnpinnedGC \/ (\A c \in RowCells(b) : root[c] >= b))
              /\ present' = present \ {b}
              /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                   stage, proposedVersion, proposedHead, proposedEpoch,
                   complete, proof, ack, visible, root,
                   badSelection, badPrefix, badContents>>
Next == (\E b \in Bundles : Prepare(b) \/ Upload(b, TRUE) \/ Upload(b, FALSE)
                            \/ Select(b) \/ Rebase(b) \/ Reconcile(b) \/ Collect(b))
        \/ (\E c \in Cells : Close(c) \/ Transfer(c)
              \/ (\E b \in Bundles : Ack(c, b) \/ Read(c, b) \/ Materialize(c, b)))
        \/ FenceNode
Spec == Init /\ [][Next]_vars
CellFence == ~badSelection
ContiguousSelection == ~badPrefix
CompleteSelection == ~badContents
ProofSelected == \A b \in proof : stage[b] = 3
ReadProven == \A c \in Cells : visible[c] = 0 \/ visible[c] \in proof
(* A selected range, including an ACK not yet rooted, stays reconstructible.
   The dormant sibling keeps the entire shared object live. Root represents
   a separately authenticated checkpoint, including identical retry outcomes. *)
ColdRecoverable == \A b \in Bundles : stage[b] # 3 \/
                    (b \in complete /\ (b \in present \/ \A c \in RowCells(b) : root[c] >= b))
ClosedPrefixFrozen == \A c \in closed : CellSelected(c) <= endpoint[c]
AckRecoverable == \A c \in Cells : ack[c] = 0 \/ ack[c] \in proof
=============================================================================
