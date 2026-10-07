------------------------- MODULE WriteProofs -------------------------
EXTENDS Naturals, Integers, FiniteSets
CONSTANTS NodeOnlySelector, IgnoreDurableFence, WallOnlyExpiry
VARIABLES mono, wall, boot, grantBoot, grantUntil, grant,
          accepted, sealed, covered, retired, grantViolation,
          cellEpoch, catalogEpoch, selectorVersion, proposedVersion,
          proposedCell, uploaded, selected, closedEndpoint, badBucketAck
vars == <<mono, wall, boot, grantBoot, grantUntil, grant,
          accepted, sealed, covered, retired, grantViolation,
          cellEpoch, catalogEpoch, selectorVersion, proposedVersion,
          proposedCell, uploaded, selected, closedEndpoint, badBucketAck>>
Init == /\ mono = 0 /\ wall = 0 /\ boot = 1
        /\ grantBoot = 0 /\ grantUntil = 0 /\ grant = FALSE
        /\ accepted = 0 /\ sealed = -1 /\ covered = 0 /\ retired = FALSE
        /\ grantViolation = FALSE
        /\ cellEpoch = 1 /\ catalogEpoch = 1 /\ selectorVersion = 1
        /\ proposedVersion = 0 /\ proposedCell = 0 /\ uploaded = FALSE
        /\ selected = 0 /\ closedEndpoint = -1 /\ badBucketAck = FALSE
Issue == /\ ~retired /\ sealed = -1 /\ mono < 3
         /\ grant' = TRUE /\ grantBoot' = boot /\ grantUntil' = mono + 1
         /\ UNCHANGED <<mono, wall, boot, accepted, sealed, covered, retired,
              grantViolation, cellEpoch, catalogEpoch, selectorVersion,
              proposedVersion, proposedCell, uploaded, selected,
              closedEndpoint, badBucketAck>>
Append == LET clock == IF WallOnlyExpiry THEN wall ELSE mono
          IN /\ grant /\ grantBoot = boot /\ clock < grantUntil /\ accepted < 2
             /\ (IgnoreDurableFence \/ (~retired /\ sealed = -1))
             /\ accepted' = accepted + 1
             /\ grantViolation' = (grantViolation \/ mono >= grantUntil)
             /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant,
                  sealed, covered, retired, cellEpoch, catalogEpoch,
                  selectorVersion, proposedVersion, proposedCell, uploaded,
                  selected, closedEndpoint, badBucketAck>>
Seal == /\ sealed = -1 /\ sealed' = accepted
        /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant, accepted,
              covered, retired, grantViolation, cellEpoch, catalogEpoch,
              selectorVersion, proposedVersion, proposedCell, uploaded,
              selected, closedEndpoint, badBucketAck>>
(* Lost seal is stuttering: no receipt or retirement authority is created. *)
Cover == /\ covered < accepted /\ covered' = accepted
         /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant, accepted,
              sealed, retired, grantViolation, cellEpoch, catalogEpoch,
              selectorVersion, proposedVersion, proposedCell, uploaded,
              selected, closedEndpoint, badBucketAck>>
Retire == /\ ~retired /\ covered = accepted
          /\ retired' = TRUE
          /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant, accepted,
               sealed, covered, grantViolation, cellEpoch, catalogEpoch,
               selectorVersion, proposedVersion, proposedCell, uploaded,
               selected, closedEndpoint, badBucketAck>>
Restart == /\ boot = 1 /\ boot' = 2 /\ grant' = FALSE
           /\ UNCHANGED <<mono, wall, grantBoot, grantUntil, accepted, sealed,
                covered, retired, grantViolation, cellEpoch, catalogEpoch,
                selectorVersion, proposedVersion, proposedCell, uploaded,
                selected, closedEndpoint, badBucketAck>>
Tick == /\ mono < 3 /\ mono' = mono + 1 /\ wall' = wall + 1
        /\ UNCHANGED <<boot, grantBoot, grantUntil, grant, accepted, sealed,
             covered, retired, grantViolation, cellEpoch, catalogEpoch,
             selectorVersion, proposedVersion, proposedCell, uploaded,
             selected, closedEndpoint, badBucketAck>>
Rollback == /\ wall > 0 /\ wall' = 0
            /\ UNCHANGED <<mono, boot, grantBoot, grantUntil, grant, accepted,
                 sealed, covered, retired, grantViolation, cellEpoch, catalogEpoch,
                 selectorVersion, proposedVersion, proposedCell, uploaded,
                 selected, closedEndpoint, badBucketAck>>
Prepare == /\ ~uploaded /\ catalogEpoch = cellEpoch /\ closedEndpoint = -1
           /\ proposedVersion' = selectorVersion /\ proposedCell' = cellEpoch
           /\ uploaded' = TRUE
           /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant, accepted,
                sealed, covered, retired, grantViolation, cellEpoch, catalogEpoch,
                selectorVersion, selected, closedEndpoint, badBucketAck>>
Select == /\ uploaded /\ selected = 0
          /\ (NodeOnlySelector \/ (proposedVersion = selectorVersion
                   /\ proposedCell = catalogEpoch /\ closedEndpoint = -1))
          /\ selected' = 1
          /\ badBucketAck' = (badBucketAck \/ proposedCell # cellEpoch)
          /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant, accepted,
               sealed, covered, retired, grantViolation, cellEpoch, catalogEpoch,
               selectorVersion, proposedVersion, proposedCell, uploaded, closedEndpoint>>
CloseBinding == /\ closedEndpoint = -1
                /\ closedEndpoint' = selected
                /\ catalogEpoch' = 0 /\ selectorVersion' = selectorVersion + 1
                /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant,
                     accepted, sealed, covered, retired, grantViolation, cellEpoch,
                     proposedVersion, proposedCell, uploaded, selected, badBucketAck>>
Transfer == /\ cellEpoch = 1 /\ closedEndpoint >= 0
            /\ cellEpoch' = 2
            /\ UNCHANGED <<mono, wall, boot, grantBoot, grantUntil, grant, accepted,
                 sealed, covered, retired, grantViolation, catalogEpoch,
                 selectorVersion, proposedVersion, proposedCell, uploaded,
                 selected, closedEndpoint, badBucketAck>>
Next == Issue \/ Append \/ Seal \/ Cover \/ Retire \/ Restart \/ Tick \/ Rollback
        \/ Prepare \/ Select \/ CloseBinding \/ Transfer
Spec == Init /\ [][Next]_vars
FrozenTail == sealed = -1 \/ accepted <= sealed
RetiredCovered == ~retired \/ accepted <= covered
GrantLifetime == ~grantViolation
BucketCellFence == ~badBucketAck
ClosedEndpointComplete == closedEndpoint = -1 \/ selected <= closedEndpoint
=============================================================================
