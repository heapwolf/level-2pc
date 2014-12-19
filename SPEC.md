# SYNOPSIS
Specification for a [`two-phase commit`][1] protocol.

# SPECIFICATION

## REQUEST Phase
### 1. COORDINATOR

- A PEER becomes a COORDINATOR when an operation is performed against it.

- To achieve [`atomicity`][2], it will coordinate a [`quorum`][3]

  A. Send each PEER a REQUEST packet with `{ data, callback-site }`.

### 2. PEER

- If a COORDINATOR is lost, the PEER will attempt to reconnect to the callback-site
  for a maximum of `N` times.

- All peers must respond to the COORDINATOR

  A. `{ data, callback-site }` is written to a temporary store for [`durability`][4].
  B. The PEERs subsequent transactions are queued to achieve [`isolation`][5].
  C. The PEER responds to the COORDINATOR
    a. If yes, the PEER sends an `AGREEMENT_YES` response.
    b. If no, the PEER sends an `AGREEMENT_ABORT` response.

## COMMIT Phase

### Fail

- A timed out response from a PEER is equivalent to `AGREEMENT_ABORT`
- If *any* response is `AGREEMENT_ABORT`

  A. An ABORT message is sent to all PEERs, the temporary store is purged
     and an `ACK_FAIL` response is sent to the COORDINATOR.
  B. ABORT is the [`consistent`][6] result of the transaction.

### Success
- If *all* responses are `AGREEMENT_YES`

  A. A `ACK_SUCCESS` message is sent to all the PEERS so that they can commit 
     the data that has been added to the temporary store.
  B. If all PEERS respond with `ACK_SUCCESS`
  C. SUCCESS is the [`consistent`][6] result of the transaction.

