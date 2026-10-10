------------------------- MODULE BindingDrain -------------------------
EXTENDS Naturals, Integers
CONSTANTS SelectedOnlyClose, IgnoreIssuanceClose, DropUncoveredFollowers
VARIABLES phase, version, issued, follower, followerPresent, ack,
          selected, objectsPresent, root, closeThrough, terminal,
          stage, candidate, candidateVersion, candidateBase
vars == <<phase, version, issued, follower, followerPresent, ack,
          selected, objectsPresent, root, closeThrough, terminal,
          stage, candidate, candidateVersion, candidateBase>>
Init == /\ phase = "Open" /\ version = 0 /\ issued = 0
        /\ follower = 0 /\ followerPresent = TRUE /\ ack = 0
        /\ selected = 0 /\ objectsPresent = TRUE /\ root = 0
        /\ closeThrough = -1 /\ terminal = -1
        /\ stage = "Empty" /\ candidate = 0
        /\ candidateVersion = -1 /\ candidateBase = -1

(* Issue abstracts a complete verified native capture assigned by the ordered
   lane. BeginClose requires the actor to quiesce SQL and join dispatched work
   before freezing this endpoint; a sampled follower watermark cannot replace
   that barrier. Followers represent the complete required native member set. *)
Issue == /\ issued < 2 /\ phase # "Transferred"
         /\ (IgnoreIssuanceClose \/ phase = "Open")
         /\ issued' = issued + 1
         /\ UNCHANGED <<phase, version, follower, followerPresent, ack,
              selected, objectsPresent, root, closeThrough, terminal,
              stage, candidate, candidateVersion, candidateBase>>
Replicate == /\ followerPresent /\ follower < issued
             /\ phase \in {"Open", "Closing"}
             /\ follower' = follower + 1
             /\ UNCHANGED <<phase, version, issued, followerPresent, ack,
                  selected, objectsPresent, root, closeThrough, terminal,
                  stage, candidate, candidateVersion, candidateBase>>
FleetAck == /\ followerPresent /\ ack < follower
            /\ phase \in {"Open", "Closing"}
            /\ ack' = ack + 1
            /\ UNCHANGED <<phase, version, issued, follower, followerPresent,
                 selected, objectsPresent, root, closeThrough, terminal,
                 stage, candidate, candidateVersion, candidateBase>>
BeginClose == /\ phase = "Open"
              /\ phase' = "Closing" /\ closeThrough' = issued
              /\ version' = version + 1
              /\ UNCHANGED <<issued, follower, followerPresent, ack,
                   selected, objectsPresent, root, terminal,
                   stage, candidate, candidateVersion, candidateBase>>

(* Closing rejects new issuance, but permits object coverage of its exact
   previously assigned prefix. A late old CAS still loses; rebase verifies both
   the frozen range and the unchanged predecessor. No new owner exists yet. *)
CanDrain == phase = "Open" \/ (phase = "Closing" /\ selected < closeThrough)
Prepare == /\ CanDrain /\ stage = "Empty" /\ selected < issued
           /\ stage' = "Prepared" /\ candidate' = selected + 1
           /\ candidateVersion' = version /\ candidateBase' = selected
           /\ UNCHANGED <<phase, version, issued, follower, followerPresent,
                ack, selected, objectsPresent, root, closeThrough, terminal>>
Upload == /\ stage = "Prepared" /\ stage' = "Uploaded"
          /\ UNCHANGED <<phase, version, issued, follower, followerPresent,
               ack, selected, objectsPresent, root, closeThrough, terminal,
               candidate, candidateVersion, candidateBase>>
Rebase == /\ CanDrain /\ stage = "Uploaded"
          /\ candidate = selected + 1 /\ candidateBase = selected
          /\ candidateVersion # version
          /\ (phase # "Closing" \/ candidate <= closeThrough)
          /\ candidateVersion' = version
          /\ UNCHANGED <<phase, version, issued, follower, followerPresent,
               ack, selected, objectsPresent, root, closeThrough, terminal,
               stage, candidate, candidateBase>>
Select == /\ CanDrain /\ stage = "Uploaded" /\ candidate <= issued
          /\ candidateVersion = version /\ candidateBase = selected
          /\ candidate = selected + 1
          /\ (phase # "Closing" \/ candidate <= closeThrough)
          /\ selected' = candidate /\ objectsPresent' = TRUE
          /\ stage' = "Empty" /\ version' = version + 1
          /\ UNCHANGED <<phase, issued, follower, followerPresent, ack,
               root, closeThrough, terminal, candidate, candidateVersion, candidateBase>>
FinishClose == /\ phase = "Closing"
               /\ (SelectedOnlyClose \/ selected >= closeThrough)
               /\ phase' = "Closed" /\ terminal' = selected
               /\ version' = version + 1
               /\ UNCHANGED <<issued, follower, followerPresent, ack,
                    selected, objectsPresent, root, closeThrough,
                    stage, candidate, candidateVersion, candidateBase>>
(* Root abstracts an authenticated independently recoverable checkpoint,
   including the exact retry outcomes. Transfer reconstructs that terminal
   endpoint before exposing a new writer; ordinary bundle references are not
   independent checkpoints. Byte verification remains a production obligation. *)
Checkpoint == /\ objectsPresent /\ root < selected
              /\ root' = selected /\ version' = version + 1
              /\ UNCHANGED <<phase, issued, follower, followerPresent, ack,
                   selected, objectsPresent, closeThrough, terminal,
                   stage, candidate, candidateVersion, candidateBase>>
Transfer == /\ phase = "Closed" /\ root >= terminal
            /\ phase' = "Transferred"
            /\ UNCHANGED <<version, issued, follower, followerPresent, ack,
                 selected, objectsPresent, root, closeThrough, terminal,
                 stage, candidate, candidateVersion, candidateBase>>
RetireFollower == /\ followerPresent
                  /\ ((DropUncoveredFollowers /\ phase = "Closing")
                      \/ (phase \in {"Closed", "Transferred"}
                          /\ selected >= issued))
                  /\ followerPresent' = FALSE
                  /\ UNCHANGED <<phase, version, issued, follower, ack,
                       selected, objectsPresent, root, closeThrough, terminal,
                       stage, candidate, candidateVersion, candidateBase>>
Collect == /\ objectsPresent /\ root >= selected
           /\ objectsPresent' = FALSE
           /\ UNCHANGED <<phase, version, issued, follower, followerPresent,
                ack, selected, root, closeThrough, terminal,
                stage, candidate, candidateVersion, candidateBase>>
Next == Issue \/ Replicate \/ FleetAck \/ BeginClose \/ Prepare \/ Upload
        \/ Rebase \/ Select \/ FinishClose \/ Checkpoint \/ Transfer
        \/ RetireFollower \/ Collect
Spec == Init /\ [][Next]_vars
IssuedPrefixFrozen == phase = "Open" \/ issued <= closeThrough
ClosedAcceptedCovered == phase \notin {"Closed", "Transferred"} \/ selected >= closeThrough
TerminalFrozen == terminal = -1 \/ selected <= terminal
AckRecoverable == ack <= root \/ (objectsPresent /\ ack <= selected)
                  \/ (followerPresent /\ ack <= follower)
TransferredRecoverable == phase # "Transferred" \/ ack <= root
SelectedRecoverable == selected <= root \/ objectsPresent
OrderedBounds == 0 <= ack /\ ack <= follower /\ follower <= issued
                 /\ root <= selected /\ selected <= issued
FairSpec == Spec /\ WF_vars(Prepare) /\ WF_vars(Upload) /\ WF_vars(Rebase)
            /\ WF_vars(Select) /\ WF_vars(FinishClose)
ClosingDrains == [](phase = "Closing" => <>(phase \in {"Closed", "Transferred"}))
=============================================================================
