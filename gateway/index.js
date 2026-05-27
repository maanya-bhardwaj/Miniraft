// ============================================================
//  Mini-RAFT Gateway
//  - Accepts WebSocket connections from browser clients
//  - Discovers and tracks the current RAFT leader
//  - Forwards stroke events to the leader
//  - Broadcasts committed strokes to all connected clients
//  - Auto-reroutes on leader failover without client disconnection
// ============================================================

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const bodyParser = require("body-parser");
const axios = require("axios");

const app = express();
app.use(bodyParser.json());

const PORT = parseInt(process.env.PORT || "3000");
const REPLICA_URLS = (process.env.REPLICAS || "")
  .split(",")
  .filter(Boolean);

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ─── State ──────────────────────────────────────────────────
let currentLeaderUrl = null;
let clients = new Set();

// ─── Logging helper ─────────────────────────────────────────
function log_(msg) {
  console.log(`[GATEWAY] ${msg}`);
}

// ─── Leader Discovery ────────────────────────────────────────
async function discoverLeader() {
  for (const url of REPLICA_URLS) {
    try {
      const res = await axios.get(`${url}/leader-info`, { timeout: 500 });
      const { leaderId, state } = res.data;

      if (state === "leader") {
        if (currentLeaderUrl !== url) {
          log_(`Leader found at ${url} (${leaderId})`);
          currentLeaderUrl = url;
        }
        return url;
      }

      // If this replica knows who the leader is, find that URL
      if (leaderId) {
        const leaderUrl = REPLICA_URLS.find((u) => u.includes(leaderId));
        if (leaderUrl) {
          try {
            const leaderRes = await axios.get(`${leaderUrl}/leader-info`, { timeout: 500 });
            if (leaderRes.data.state === "leader") {
              if (currentLeaderUrl !== leaderUrl) {
                log_(`Leader resolved at ${leaderUrl} (${leaderId})`);
                currentLeaderUrl = leaderUrl;
              }
              return leaderUrl;
            }
          } catch (_) {}
        }
      }
    } catch (_) {
      log_(`Replica ${url} unreachable`);
    }
  }

  log_(`No leader found — election may be in progress`);
  currentLeaderUrl = null;
  return null;
}

// Poll for leader every 500ms
setInterval(discoverLeader, 500);
discoverLeader(); // initial discovery

// ─── Forward stroke to leader ────────────────────────────────
async function forwardStroke(stroke) {
  let attempts = 0;
  while (attempts < 5) {
    let leaderUrl = currentLeaderUrl;
    if (!leaderUrl) {
      leaderUrl = await discoverLeader();
    }
    if (!leaderUrl) {
      await sleep(200);
      attempts++;
      continue;
    }

    try {
      const res = await axios.post(`${leaderUrl}/stroke`, stroke, { timeout: 1000 });
      if (res.data.ok) return true;

      // Leader redirected (stale)
      if (res.data.redirect) {
        currentLeaderUrl = null;
        attempts++;
        continue;
      }
    } catch (err) {
      log_(`Failed to send stroke to ${leaderUrl}: ${err.message}`);
      currentLeaderUrl = null;
    }
    attempts++;
    await sleep(100);
  }
  return false;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── WebSocket Handler ───────────────────────────────────────
wss.on("connection", (ws) => {
  clients.add(ws);
  log_(`Client connected (total=${clients.size})`);

  // Send current leader info
  ws.send(JSON.stringify({ type: "leader", leader: currentLeaderUrl }));

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === "stroke") {
        const ok = await forwardStroke(msg.stroke);
        if (!ok) {
          ws.send(JSON.stringify({ type: "error", message: "Could not commit stroke" }));
        }
      }
    } catch (err) {
      log_(`Message parse error: ${err.message}`);
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    log_(`Client disconnected (total=${clients.size})`);
  });

  ws.on("error", (err) => {
    log_(`WebSocket error: ${err.message}`);
    clients.delete(ws);
  });
});

// ─── Broadcast committed stroke to all clients ───────────────
app.post("/committed-stroke", (req, res) => {
  const { stroke, index } = req.body;
  log_(`Broadcasting committed stroke index=${index} to ${clients.size} clients`);

  const msg = JSON.stringify({ type: "stroke", stroke, index });
  clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });

  res.json({ ok: true, delivered: clients.size });
});

// ─── Health & Status ─────────────────────────────────────────
app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/status", (req, res) => {
  res.json({
    clients: clients.size,
    leader: currentLeaderUrl,
    replicas: REPLICA_URLS,
  });
});

// ─── Boot ────────────────────────────────────────────────────
server.listen(PORT, () => {
  log_(`Gateway listening on port ${PORT}`);
  log_(`Replicas: ${REPLICA_URLS.join(", ")}`);
});

process.on("SIGTERM", () => {
  log_("SIGTERM: shutting down gracefully");
  wss.close();
  server.close();
  process.exit(0);
});
