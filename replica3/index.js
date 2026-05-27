// ============================================================
//  Mini-RAFT Replica Node
//  Implements: Follower / Candidate / Leader state machine
//  Exposes:    /request-vote, /append-entries, /heartbeat,
//              /sync-log, /stroke (from Gateway), /health,
//              /status (debug)
// ============================================================

const express = require("express");
const bodyParser = require("body-parser");
const axios = require("axios");

const app = express();
app.use(bodyParser.json());

// ─── Config ────────────────────────────────────────────────
const REPLICA_ID = process.env.REPLICA_ID || "replica1";
const PORT = parseInt(process.env.PORT || "4001");
const PEERS = (process.env.PEERS || "").split(",").filter(Boolean);
const GATEWAY_URL = process.env.GATEWAY_URL || "http://gateway:3000";

// ─── RAFT Constants ─────────────────────────────────────────
const HEARTBEAT_INTERVAL = 150;          // ms
const ELECTION_TIMEOUT_MIN = 500;        // ms
const ELECTION_TIMEOUT_MAX = 800;        // ms

// ─── State ──────────────────────────────────────────────────
let state = "follower";                  // follower | candidate | leader
let currentTerm = 0;
let votedFor = null;                     // nodeId we voted for this term
let log = [];                            // [ { index, term, stroke } ]
let commitIndex = -1;                    // highest committed log index
let lastApplied = -1;

// Leader-only tracking
let nextIndex = {};                      // peer → next log index to send
let matchIndex = {};                     // peer → highest confirmed index

let currentLeader = null;
let electionTimer = null;
let heartbeatTimer = null;

// ─── Logging helper ─────────────────────────────────────────
function log_(msg) {
  console.log(`[${REPLICA_ID}][term=${currentTerm}][${state.toUpperCase()}] ${msg}`);
}

// ─── Election timer ─────────────────────────────────────────
function resetElectionTimer() {
  clearTimeout(electionTimer);
  const timeout =
    ELECTION_TIMEOUT_MIN +
    Math.floor(Math.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN));
  electionTimer = setTimeout(startElection, timeout);
}

function stopElectionTimer() {
  clearTimeout(electionTimer);
}

// ─── Become Follower ─────────────────────────────────────────
function becomeFollower(term, leaderId = null) {
  log_(`Becoming FOLLOWER (term=${term}, leader=${leaderId})`);
  state = "follower";
  currentTerm = term;
  votedFor = null;
  currentLeader = leaderId;
  clearInterval(heartbeatTimer);
  resetElectionTimer();
}

// ─── Start Election ──────────────────────────────────────────
async function startElection() {
  state = "candidate";
  currentTerm += 1;
  votedFor = REPLICA_ID;
  let votes = 1; // vote for self
  log_(`Starting election`);

  const lastLogIndex = log.length - 1;
  const lastLogTerm = lastLogIndex >= 0 ? log[lastLogIndex].term : -1;

  const voteRequests = PEERS.map(async (peer) => {
    try {
      const res = await axios.post(
        `${peer}/request-vote`,
        {
          term: currentTerm,
          candidateId: REPLICA_ID,
          lastLogIndex,
          lastLogTerm,
        },
        { timeout: 300 }
      );
      if (res.data.voteGranted) {
        votes += 1;
        log_(`Vote granted by ${peer} (total=${votes})`);
      } else if (res.data.term > currentTerm) {
        becomeFollower(res.data.term);
      }
    } catch (_) {
      log_(`Vote request to ${peer} failed`);
    }
  });

  await Promise.allSettled(voteRequests);

  if (state !== "candidate") return; // stepped down mid-election

  const majority = Math.floor((PEERS.length + 1) / 2) + 1;
  if (votes >= majority) {
    becomeLeader();
  } else {
    log_(`Election lost (votes=${votes}/${PEERS.length + 1}), retrying...`);
    becomeFollower(currentTerm);
  }
}

// ─── Become Leader ───────────────────────────────────────────
function becomeLeader() {
  log_(`Became LEADER`);
  state = "leader";
  currentLeader = REPLICA_ID;
  stopElectionTimer();

  PEERS.forEach((peer) => {
    nextIndex[peer] = log.length;
    matchIndex[peer] = -1;
  });

  clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_INTERVAL);
  sendHeartbeats(); // immediate first heartbeat
}

// ─── Heartbeats ──────────────────────────────────────────────
async function sendHeartbeats() {
  if (state !== "leader") return;

  PEERS.forEach(async (peer) => {
    try {
      const res = await axios.post(
        `${peer}/heartbeat`,
        { term: currentTerm, leaderId: REPLICA_ID, commitIndex },
        { timeout: 200 }
      );
      if (res.data.term > currentTerm) {
        becomeFollower(res.data.term);
      }
    } catch (_) {
      // peer unreachable — normal during restarts
    }
  });
}

// ─── Log Replication ─────────────────────────────────────────
async function replicateEntry(entry) {
  if (state !== "leader") return false;

  const acks = new Set([REPLICA_ID]); // leader self-acks
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;

  const replicateToPeer = async (peer) => {
    const ni = nextIndex[peer] ?? 0;
    const prevLogIndex = ni - 1;
    const prevLogTerm = prevLogIndex >= 0 && log[prevLogIndex] ? log[prevLogIndex].term : -1;
    const entries = log.slice(ni);

    try {
      const res = await axios.post(
        `${peer}/append-entries`,
        { term: currentTerm, leaderId: REPLICA_ID, prevLogIndex, prevLogTerm, entries, leaderCommit: commitIndex },
        { timeout: 400 }
      );

      if (res.data.success) {
        matchIndex[peer] = log.length - 1;
        nextIndex[peer] = log.length;
        acks.add(peer);
      } else if (res.data.term > currentTerm) {
        becomeFollower(res.data.term);
      } else {
        // Consistency check failed: back off nextIndex
        nextIndex[peer] = Math.max(0, (nextIndex[peer] ?? 0) - 1);
        // Send sync
        await syncFollower(peer, res.data.logLength || 0);
        acks.add(peer);
      }
    } catch (_) {
      log_(`AppendEntries to ${peer} failed`);
    }
  };

  await Promise.allSettled(PEERS.map(replicateToPeer));
  return acks.size >= majority;
}

// ─── Sync Follower (catch-up) ────────────────────────────────
async function syncFollower(peer, fromIndex) {
  const missing = log.slice(fromIndex).filter((e) => e.index <= commitIndex);
  if (missing.length === 0) return;
  try {
    await axios.post(`${peer}/sync-log`, { entries: missing, commitIndex }, { timeout: 1000 });
    log_(`Synced ${missing.length} entries to ${peer} from index ${fromIndex}`);
  } catch (_) {
    log_(`Sync to ${peer} failed`);
  }
}

// ─── Commit advancement ──────────────────────────────────────
function advanceCommitIndex() {
  if (state !== "leader") return;
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;
  for (let n = log.length - 1; n > commitIndex; n--) {
    if (log[n] && log[n].term === currentTerm) {
      const ackCount = 1 + PEERS.filter((p) => (matchIndex[p] ?? -1) >= n).length;
      if (ackCount >= majority) {
        commitIndex = n;
        log_(`Committed log index ${commitIndex}`);
        break;
      }
    }
  }
}

// ─── Routes ──────────────────────────────────────────────────

// Health check
app.get("/health", (req, res) => res.json({ ok: true }));

// Debug status
app.get("/status", (req, res) => {
  res.json({
    id: REPLICA_ID,
    state,
    term: currentTerm,
    leader: currentLeader,
    logLength: log.length,
    commitIndex,
    votedFor,
  });
});

// ── /request-vote ────────────────────────────────────────────
app.post("/request-vote", (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;
  log_(`Vote requested by ${candidateId} term=${term}`);

  if (term > currentTerm) {
    becomeFollower(term);
  }

  let voteGranted = false;
  const myLastLogIndex = log.length - 1;
  const myLastLogTerm = myLastLogIndex >= 0 ? log[myLastLogIndex].term : -1;

  const logOk =
    lastLogTerm > myLastLogTerm ||
    (lastLogTerm === myLastLogTerm && lastLogIndex >= myLastLogIndex);

  if (term >= currentTerm && (votedFor === null || votedFor === candidateId) && logOk) {
    votedFor = candidateId;
    voteGranted = true;
    resetElectionTimer();
    log_(`Granted vote to ${candidateId}`);
  }

  res.json({ term: currentTerm, voteGranted });
});

// ── /heartbeat ───────────────────────────────────────────────
app.post("/heartbeat", (req, res) => {
  const { term, leaderId, commitIndex: leaderCommit } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false });
  }

  if (term > currentTerm || state !== "follower") {
    becomeFollower(term, leaderId);
  } else {
    currentLeader = leaderId;
    resetElectionTimer();
  }

  // Apply any commits we haven't applied
  if (leaderCommit !== undefined && leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, log.length - 1);
  }

  res.json({ term: currentTerm, success: true });
});

// ── /append-entries ──────────────────────────────────────────
app.post("/append-entries", (req, res) => {
  const { term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false, logLength: log.length });
  }

  becomeFollower(term, leaderId);

  // Consistency check
  if (prevLogIndex >= 0) {
    if (!log[prevLogIndex] || log[prevLogIndex].term !== prevLogTerm) {
      log_(`Consistency check failed at prevLogIndex=${prevLogIndex}`);
      return res.json({ term: currentTerm, success: false, logLength: log.length });
    }
  }

  // Append new entries (overwrite conflicts)
  if (entries && entries.length > 0) {
    log = log.slice(0, prevLogIndex + 1).concat(entries);
    log_(`Appended ${entries.length} entries, log length=${log.length}`);
  }

  if (leaderCommit !== undefined && leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, log.length - 1);
  }

  res.json({ term: currentTerm, success: true, logLength: log.length });
});

// ── /sync-log ────────────────────────────────────────────────
app.post("/sync-log", (req, res) => {
  const { entries, commitIndex: leaderCommit } = req.body;
  if (!entries || entries.length === 0) return res.json({ ok: true });

  // Append missing entries
  const startIndex = entries[0].index;
  log = log.slice(0, startIndex).concat(entries);
  commitIndex = leaderCommit;
  log_(`Sync-log: caught up to index ${commitIndex}, log length=${log.length}`);
  res.json({ ok: true });
});

// ── /stroke ── (called by Gateway when this node is leader) ──
app.post("/stroke", async (req, res) => {
  if (state !== "leader") {
    return res.status(302).json({ redirect: currentLeader });
  }

  const stroke = req.body;
  const entry = { index: log.length, term: currentTerm, stroke };
  log.push(entry);
  log_(`Received stroke, appending at index ${entry.index}`);

  const committed = await replicateEntry(entry);
  if (committed) {
    advanceCommitIndex();
    // Notify gateway to broadcast
    try {
      await axios.post(`${GATEWAY_URL}/committed-stroke`, { stroke, index: entry.index }, { timeout: 500 });
    } catch (_) {
      log_(`Failed to notify gateway of committed stroke`);
    }
    res.json({ ok: true, index: entry.index });
  } else {
    // Didn't get majority — remove from log and reject
    log.pop();
    log_(`Stroke rejected: could not achieve majority`);
    res.status(500).json({ error: "Could not achieve majority" });
  }
});

// ── /leader-info ─────────────────────────────────────────────
app.get("/leader-info", (req, res) => {
  res.json({ leaderId: currentLeader, state, term: currentTerm });
});

// ─── Boot ────────────────────────────────────────────────────
app.listen(PORT, () => {
  log_(`Replica listening on port ${PORT}`);
  log_(`Peers: ${PEERS.join(", ")}`);
  resetElectionTimer();
});

// Graceful shutdown
process.on("SIGTERM", () => {
  log_("SIGTERM: shutting down gracefully");
  clearTimeout(electionTimer);
  clearInterval(heartbeatTimer);
  process.exit(0);
});
