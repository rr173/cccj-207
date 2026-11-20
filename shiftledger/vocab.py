"""Stable machine-readable vocabulary tokens for the handover ledger.

Scripts can rely on these constants; the Chinese-facing documentation in
``docs/HANDOVER.md`` explains each one.  Error types live in
:mod:`shiftledger.errors`.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Group attribution outcomes (groups = (sender, control_id, obr_index)).
# ---------------------------------------------------------------------------
GRP_ASSIGNED = "assigned"                  # uniquely attributed (auto/manual)
GRP_AMBIGUOUS = "ambiguous"                # >1 candidate work orders remain
GRP_NO_PATIENT = "no_patient_identity"     # observation rows carry no patient id
GRP_NO_ORDER = "no_order_number"           # neither placer nor filler present
GRP_UNREGISTERED_ORDER = "unregistered_order"  # a provided number is unmapped
GRP_PATIENT_MISMATCH = "patient_mismatch"  # patient domain/id not on candidates
GRP_MAPPING_CONFLICT = "mapping_conflict"  # provided numbers map to disjoint sets
GRP_OUT_OF_WINDOW = "out_of_window"        # mapping not effective at received_at

# Per-number mapping detail reasons.
NUM_MAPPED = "mapped"
NUM_NO_MAPPING = "no_mapping"
NUM_NOT_EFFECTIVE = "not_effective"

# ---------------------------------------------------------------------------
# Observation dispositions (every included observation has exactly one).
# ---------------------------------------------------------------------------
OBS_ADOPTED = "adopted"                  # consumed by an item (auto or locked)
OBS_GROUP_PENDING = "group_pending"      # its group's attribution is undecided
OBS_UNATTRIBUTABLE = "unattributable"    # group cannot be attributed at all
OBS_CANDIDATE_UNASSIGNED = "candidate_unassigned"  # has matching free items
OBS_ITEMS_OCCUPIED = "matching_items_occupied"      # matches only occupied items
OBS_NO_MATCHING_ITEM = "no_matching_item"  # no receivable item matches in WO

# ---------------------------------------------------------------------------
# Receivable item statuses.
# ---------------------------------------------------------------------------
ITEM_AUTO_SETTLED = "auto_settled"       # written off by the unique 1:1 match
ITEM_LOCK_SETTLED = "lock_settled"       # written off by a manual lock
ITEM_PENDING_DECISION = "pending_decision"  # open, candidates / ambiguity exist
ITEM_PENDING_RECEIPT = "pending_receipt"    # open, no candidate, cutoff <= due
ITEM_OVERDUE = "overdue"                    # open, no candidate, cutoff > due

# Timeliness of a settled item.
SETTLED_ON_TIME = "on_time"
SETTLED_LATE = "late"

# Edge kinds stored per version.
EDGE_AUTO = "auto"
EDGE_LOCK = "lock"
EDGE_CANDIDATE = "candidate"        # obs in attributed group -> free item
EDGE_POTENTIAL = "potential"        # obs in ambiguous group -> possible item

# Decision record actions.
DEC_ASSIGN_GROUP = "assign_group"
DEC_WITHDRAW_GROUP = "withdraw_group"
DEC_LOCK_OBS = "lock_observation"
DEC_WITHDRAW_LOCK = "withdraw_lock"
