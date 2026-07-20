import express from "express";
import https from "node:https";

const app = express();
app.use(express.json({ limit: "64kb" }));
app.use(express.text({ type: "*/*", limit: "64kb" }));
app.use(express.static("public"));

const PORT = Number(process.env.PORT || 8080);
const AUTH_TOKEN = process.env.AUTH_TOKEN || "";
const PRICE_POLL_MS = Number(process.env.PRICE_POLL_MS || 5000);
const state = new Map();
const clients = new Set();

function normSymbol(raw) {
  if (!raw || typeof raw !== "string") return null;
  let symbol = raw.trim();
  if (symbol.includes(":")) symbol = symbol.split(":").pop();
  if (symbol.endsWith(".P")) symbol = symbol.slice(0, -2);
  symbol = symbol.toUpperCase().replace(/[^A-Z0-9_-]/g, "");
  return symbol || null;
}

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function enrich(row) {
  const width = row.upper - row.lower;
  const positionPct = Number.isFinite(row.price) && width > 0 ? ((row.price - row.lower) / width) * 100 : null;
  let position = "NO PRICE";
  if (positionPct !== null) {
    if (positionPct > 100) position = "ABOVE";
    else if (positionPct < 0) position = "BELOW";
    else if (positionPct >= 50) position = "UPPER HALF";
    else position = "LOWER HALF";
  }
  return { ...row, position, positionPct };
}

function snapshot() {
  return Array.from(state.values()).map(enrich).sort((a, b) => a.symbol.localeCompare(b.symbol));
}

function broadcast() {
  const message = `event: update\ndata: ${JSON.stringify(snapshot())}\n\n`;
  for (const client of clients) client.write(message);
}

function parseBody(body) {
  if (typeof body !== "string") return body;
  return JSON.parse(body);
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => { body += chunk; });
      response.on("end", () => {
        if ((response.statusCode || 500) >= 400) return reject(new Error(`HTTP ${response.statusCode}`));
        try { resolve(JSON.parse(body)); } catch (error) { reject(error); }
      });
    }).on("error", reject);
  });
}

app.post("/webhook", (req, res) => {
  try {
    const body = parseBody(req.body);
    const token = req.headers["x-auth-token"] || body.secret || "";
    if (AUTH_TOKEN && token !== AUTH_TOKEN) return res.status(401).send("Unauthorized");

    const symbol = normSymbol(body.ticker || body.symbol);
    const renkoOpen = finite(body.renko_open ?? body.renkoOpen);
    const renkoClose = finite(body.renko_close ?? body.renkoClose);
    const suppliedUpper = finite(body.upper);
    const suppliedLower = finite(body.lower);
    const upper = suppliedUpper ?? (renkoOpen !== null && renkoClose !== null ? Math.max(renkoOpen, renkoClose) : null);
    const lower = suppliedLower ?? (renkoOpen !== null && renkoClose !== null ? Math.min(renkoOpen, renkoClose) : null);
    const suppliedDirection = finite(body.direction);
    const direction = suppliedDirection === -1 || String(body.direction).toLowerCase() === "down" || (renkoOpen !== null && renkoClose !== null && renkoClose < renkoOpen) ? -1 : 1;

    if (!symbol || upper === null || lower === null) return res.status(400).send("Missing symbol and Renko levels");

    const previous = state.get(symbol) || { symbol, price: null, priceUpdatedAt: null };
    state.set(symbol, { ...previous, symbol, upper, lower, average: finite(body.average) ?? (upper + lower) / 2, direction, timeframe: String(body.timeframe || previous.timeframe || ""), updatedAt: Date.now() });
    broadcast();
    res.sendStatus(200);
  } catch (error) {
    console.error("Webhook error:", error instanceof Error ? error.message : error);
    res.status(400).send("Invalid webhook payload");
  }
});

app.get("/api/state", (_req, res) => res.json(snapshot()));

app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  clients.add(res);
  res.write(`event: update\ndata: ${JSON.stringify(snapshot())}\n\n`);
  req.on("close", () => clients.delete(res));
});

async function pollPrices() {
  if (state.size === 0) return;
  try {
    const prices = await getJson("https://fapi.binance.com/fapi/v1/ticker/price");
    const bySymbol = new Map(prices.map((item) => [item.symbol, Number(item.price)]));
    const now = Date.now();
    for (const [symbol, row] of state) {
      const price = bySymbol.get(symbol);
      if (Number.isFinite(price)) state.set(symbol, { ...row, price, priceUpdatedAt: now });
    }
    broadcast();
  } catch (error) {
    console.error("Binance price update failed:", error instanceof Error ? error.message : error);
  }
}

setInterval(pollPrices, PRICE_POLL_MS);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Renko dashboard: http://localhost:${PORT}`);
  console.log(`TradingView webhook: http://localhost:${PORT}/webhook`);
});
