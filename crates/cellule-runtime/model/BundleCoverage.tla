------------------------ MODULE BundleCoverage ------------------------
EXTENDS Naturals, Integers, FiniteSets, TLC
CONSTANTS NodeOnly, SkipComplete, SkipPrefix, UnprovedRead, UnpinnedGC
Cells == {1, 2}
Bundles == {1, 2}
VARIABLES open, version, head, epoch, closed, endpoint,
          stage, proposedVersion, proposedHead, proposedEpoch, complete,
          present, proof, ack, visible, root, badSelection, badPrefix,
          badContents
vars == <<open, version, head, epoch, closed, endpoint,
          stage, proposedVersion, proposedHead, proposedEpoch, complete,
          present, proof, ack, visible, root, badSelection, badPrefix,
          badContents>>
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

(* Each bundle carries one exact mutation AND retry outcome for each Cell.
   Complete abstracts authentication of all bytes, outcomes and dependencies.
   It is a checked input, not an assumption about every uploaded object. *)
Prepare(b) == /\ stage[b] = 0 /\ open /\ closed = {}
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
                  /\ complete' = IF good THEN complete \cup {b} ELSE complete
                  /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                       proposedVersion, proposedHead, proposedEpoch, proof,
                       ack, visible, root, badSelection, badPrefix, badContents>>
Select(b) == /\ stage[b] = 2 /\ open /\ b \in present
             /\ (SkipComplete \/ b \in complete)
             /\ (NodeOnly \/ (proposedVersion[b] = version /\ closed = {}
                      /\ proposedEpoch[b] = epoch))
             /\ (SkipPrefix \/ (proposedHead[b] = head /\ b = head + 1))
             /\ stage' = [stage EXCEPT ![b] = 3]
             /\ head' = b /\ version' = version + 1
             /\ badSelection' = badSelection \/ closed # {} \/ proposedEpoch[b] # epoch
             /\ badPrefix' = badPrefix \/ b # head + 1 \/ proposedHead[b] # head
             /\ badContents' = badContents \/ b \notin complete
             /\ UNCHANGED <<open, epoch, closed, endpoint, proposedVersion,
                  proposedHead, proposedEpoch, complete, present, proof,
                  ack, visible, root>>

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
Ack(c, b) == /\ b \in proof /\ c \notin closed
             /\ proposedEpoch[b][c] = epoch[c] /\ ack[c] < b
             /\ ack' = [ack EXCEPT ![c] = b]
             /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                  stage, proposedVersion, proposedHead, proposedEpoch,
                  complete, present, proof, visible, root,
                  badSelection, badPrefix, badContents>>
Read(c, b) == /\ visible[c] < b /\ (UnprovedRead \/ b \in proof)
              /\ proposedEpoch[b][c] = epoch[c] /\ c \notin closed
              /\ visible' = [visible EXCEPT ![c] = b]
              /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                   stage, proposedVersion, proposedHead, proposedEpoch,
                   complete, present, proof, ack, root,
                   badSelection, badPrefix, badContents>>
Materialize(c, b) == /\ b \in proof /\ b \in present /\ b \in complete
                    /\ root[c] < b
                    /\ root' = [root EXCEPT ![c] = b]
                    /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                         stage, proposedVersion, proposedHead, proposedEpoch,
                         complete, present, proof, ack, visible,
                         badSelection, badPrefix, badContents>>
Close(c) == /\ c \notin closed
            /\ closed' = closed \cup {c}
            /\ endpoint' = [endpoint EXCEPT ![c] = head]
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
              /\ (UnpinnedGC \/ (\A c \in Cells : root[c] >= b))
              /\ present' = present \ {b}
              /\ UNCHANGED <<open, version, head, epoch, closed, endpoint,
                   stage, proposedVersion, proposedHead, proposedEpoch,
                   complete, proof, ack, visible, root,
                   badSelection, badPrefix, badContents>>
Next == (\E b \in Bundles : Prepare(b) \/ Upload(b, TRUE) \/ Upload(b, FALSE)
                            \/ Select(b) \/ Reconcile(b) \/ Collect(b))
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
                    (b \in complete /\ (b \in present \/ \A c \in Cells : root[c] >= b))
ClosedPrefixFrozen == \A c \in closed : head <= endpoint[c]
AckRecoverable == \A c \in Cells : ack[c] = 0 \/ ack[c] \in proof
=============================================================================
