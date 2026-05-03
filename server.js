const express = require("express");

const app = express();
app.use(express.json());
app.use(express.text({ type: "*/*" }));
app.use(express.static("public"));

const PORT = process.env.PORT || 8080;
const AUTH_TOKEN = process.env.AUTH_TOKEN || ""; // optional, set to protect webhook
const PRICE_POLL_MS = Number(process.env.PRICE_POLL_MS || 5000);

// In-memory state keyed by symbol
// {
//   BTCUSDT: { symbol, renkoOpen, renkoClose, trend, updatedAt, price, priceUpdatedAt }
// }
const state = new Map();

// SSE clients
const clients = new Set();
function broadcast() {
  const payload = JSON.stringify(getStateArray());
  for (const res of clients) {
    res.write(`event: update\ndata: ${payload}\n\n`);
  }
}

function normSymbol(raw) {
  if (!raw || typeof raw !== "string") return null;
  let s = raw.trim();
  if (s.includes(":")) s = s.split(":").pop();
  if (s.endsWith(".P")) s = s.slice(0, -2);
  return s.toUpperCase();
}

function computeVerdicts(row) {
  const { renkoOpen, renkoClose, price } = row;

  const v1 =
    renkoClose > renkoOpen ? "bullish" :
    renkoClose < renkoOpen ? "bearish" :
    "neutral";

  let v2 = "n/a";
  if (Number.isFinite(price) && Number.isFinite(renkoOpen) && Number.isFinite(renkoClose)) {
    if (v1 === "bullish") {
      v2 =
        price > renkoClose ? "high" :
        price > renkoOpen ? "normal" :
        "low";
    } else if (v1 === "bearish") {
      // mirror logic for bearish
      v2 =
        price < renkoClose ? "high" :
        price < renkoOpen ? "normal" :
        "low";
    } else {
      v2 = "neutral";
    }
  }

  return { verdict1: v1, verdict2: v2 };
}

function getStateArray() {
  const rows = Array.from(state.values()).map((row) => {
    const verdicts = computeVerdicts(row);
    return { ...row, ...verdicts };
  });

  rows.sort((a, b) => a.symbol.localeCompare(b.symbol));
  return rows;
}

// Webhook endpoint
app.post("/webhook", async (req, res) => {
  try {
    if (AUTH_TOKEN) {
      const token = req.headers["x-auth-token"];
      if (token !== AUTH_TOKEN) return res.status(401).send("Unauthorized");
    }

    let body = req.body;

    if (typeof body === "string") {
      try {
        body = JSON.parse(body);
      } catch {
        return res.status(400).send("Invalid JSON");
      }
    }

    const symbol = normSymbol(body.ticker || body.symbol);
    const renkoOpen = Number(body.renko_open ?? body.renkoOpen);
    const renkoClose = Number(body.renko_close ?? body.renkoClose);

    if (!symbol || !Number.isFinite(renkoOpen) || !Number.isFinite(renkoClose)) {
      return res.status(400).send("Missing fields: ticker/symbol, renko_open, renko_close");
    }

    const prev = state.get(symbol) || { symbol };

    const trend =
      renkoClose > renkoOpen ? 1 :
      renkoClose < renkoOpen ? -1 :
      0;

    state.set(symbol, {
      ...prev,
      symbol,
      renkoOpen,
      renkoClose,
      trend,
      updatedAt: Date.now(),
    });

    broadcast();
    res.sendStatus(200);
  } catch (e) {
    console.error("Webhook error:", e.message);
    res.sendStatus(500);
  }
});

// API for table
app.get("/api/state", (req, res) => {
  res.json(getStateArray());
});

// SSE for live updates
app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  clients.add(res);

  // send initial snapshot
  res.write(`event: update\ndata: ${JSON.stringify(getStateArray())}\n\n`);

  req.on("close", () => {
    clients.delete(res);
  });
});

// Binance price polling
async function pollPrices() {
  if (state.size === 0) return;

  // Build list of symbols to fetch
  const symbols = Array.from(state.keys());

  for (const symbol of symbols) {
    try {
      const url = `https://fapi.binance.com/fapi/v1/ticker/price?symbol=${encodeURIComponent(symbol)}`;
      const r = await fetch(url, { method: "GET" });
      if (!r.ok) continue;
      const data = await r.json();
      const price = Number(data.price);
      if (!Number.isFinite(price)) continue;

      const prev = state.get(symbol) || { symbol };
      state.set(symbol, {
        ...prev,
        price,
        priceUpdatedAt: Date.now(),
      });
    } catch {}
  }

  broadcast();
}

setInterval(pollPrices, PRICE_POLL_MS);

app.listen(PORT, () => {
  console.log(`Dashboard running on port ${PORT}`);
  console.log(`POST webhook: http://localhost:${PORT}/webhook`);
  console.log(`Open UI: http://localhost:${PORT}/`);
});