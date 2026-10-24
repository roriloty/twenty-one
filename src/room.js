import { DurableObject } from "cloudflare:workers";

const TARGET = 21;
const MAX_PLAYERS = 2;
const ROOM_TTL_MS = 6 * 60 * 60 * 1000; // delete idle rooms after 6h

const sum = (cards) => cards.reduce((a, b) => a + b, 0);

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = crypto.getRandomValues(new Uint32Array(1))[0] % (i + 1);
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

const sanitizeName = (n) => String(n || "").replace(/[<>]/g, "").trim().slice(0, 16) || "Player";

export class GameRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.s = null; // null until the room is created via /init
    ctx.blockConcurrencyWhile(async () => {
      this.s = (await ctx.storage.get("s")) ?? null;
    });
  }

  async save() {
    await this.ctx.storage.put("s", this.s);
    await this.ctx.storage.setAlarm(Date.now() + ROOM_TTL_MS);
  }

  async alarm() {
    if (this.ctx.getWebSockets().length === 0) {
      await this.ctx.storage.deleteAll();
      this.s = null;
    } else {
      await this.ctx.storage.setAlarm(Date.now() + ROOM_TTL_MS);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/init") {
      if (this.s) return new Response("exists", { status: 409 });
      this.s = { phase: "lobby", hostId: null, players: [], deck: [], turn: 0, passes: 0, result: null, log: [] };
      await this.save();
      return new Response("ok");
    }

    if (url.pathname === "/info") {
      if (!this.s) return Response.json({ error: "Room not found." }, { status: 404 });
      return Response.json({ players: this.s.players.length, max: MAX_PLAYERS, phase: this.s.phase });
    }

    // WebSocket upgrade
    const cid = (url.searchParams.get("cid") || "").slice(0, 64);
    const name = sanitizeName(url.searchParams.get("name"));
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);

    const reject = (message) => {
      server.send(JSON.stringify({ type: "error", fatal: true, message }));
      server.close(1008, message);
    };

    if (!this.s) reject("Room not found.");
    else if (!cid) reject("Missing client id.");
    else {
      const existing = this.s.players.find((p) => p.id === cid);
      if (existing) {
        existing.name = name;
        // replace any stale socket for this player
        for (const ws of this.ctx.getWebSockets()) {
          if (ws !== server && ws.deserializeAttachment()?.cid === cid) ws.close(1000, "replaced");
        }
      } else if (this.s.phase !== "lobby" || this.s.players.length >= MAX_PLAYERS) {
        reject("Room is full.");
      } else {
        this.s.players.push({ id: cid, name, hidden: null, visible: [] });
        if (!this.s.hostId) this.s.hostId = cid;
      }
      if (this.s.players.some((p) => p.id === cid)) {
        server.serializeAttachment({ cid });
        await this.save();
        this.broadcast();
      }
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    const cid = ws.deserializeAttachment()?.cid;
    const s = this.s;
    if (!s || !cid) return;
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    const me = s.players.find((p) => p.id === cid);
    if (!me) return;
    const err = (message) => ws.send(JSON.stringify({ type: "error", message }));

    if (msg.type === "leave") {
      // one player leaving ends the room for everyone
      for (const other of this.ctx.getWebSockets()) {
        if (other === ws) continue;
        try {
          other.send(JSON.stringify({ type: "left", message: `${me.name} left the game.` }));
          other.close(1000, "room closed");
        } catch {}
      }
      ws.close(1000, "left");
      this.s = null;
      await this.ctx.storage.deleteAlarm();
      await this.ctx.storage.deleteAll();
      return;
    }

    if (msg.type === "start") {
      if (cid !== s.hostId) return err("Only the host can start.");
      if (s.phase === "playing") return err("Game already running.");
      if (s.players.length < MAX_PLAYERS) return err("Waiting for another player.");
      this.startGame();
    } else if (msg.type === "draw") {
      if (s.phase !== "playing" || s.players[s.turn].id !== cid) return err("Not your turn.");
      if (s.deck.length === 0) return err("The deck is empty.");
      me.visible.push(s.deck.pop());
      s.passes = 0;
      s.last = { pid: cid, type: "draw", n: (s.last?.n ?? 0) + 1 };
      s.log.push(`${me.name} drew a card.`);
      if (sum([me.hidden, ...me.visible]) > TARGET) {
        const other = s.players.find((p) => p.id !== cid);
        this.finish(other.id, `${me.name} went over ${TARGET}!`);
      } else {
        s.turn = (s.turn + 1) % s.players.length;
      }
    } else if (msg.type === "pass") {
      if (s.phase !== "playing" || s.players[s.turn].id !== cid) return err("Not your turn.");
      s.passes++;
      s.last = { pid: cid, type: "pass", n: (s.last?.n ?? 0) + 1 };
      s.log.push(`${me.name} passed.`);
      if (s.passes >= s.players.length) this.reveal();
      else s.turn = (s.turn + 1) % s.players.length;
    } else {
      return;
    }

    await this.save();
    this.broadcast();
  }

  async webSocketClose(ws) {
    const cid = ws.deserializeAttachment()?.cid;
    ws.close();
    if (this.s && cid && this.s.phase === "lobby" && !this.isConnected(cid)) {
      // leaving the lobby frees the seat
      this.s.players = this.s.players.filter((p) => p.id !== cid);
      if (this.s.hostId === cid) this.s.hostId = this.s.players[0]?.id ?? null;
      await this.save();
    }
    this.broadcast();
  }

  async webSocketError(ws) {
    await this.webSocketClose(ws);
  }

  isConnected(cid) {
    return this.ctx.getWebSockets().some((ws) => ws.deserializeAttachment()?.cid === cid && ws.readyState === 1);
  }

  startGame() {
    const s = this.s;
    s.deck = shuffle(Array.from({ length: 11 }, (_, i) => i + 1)); // 1..11, each unique
    for (const p of s.players) {
      p.hidden = s.deck.pop(); // face down
      p.visible = [s.deck.pop()]; // face up
    }
    s.turn = crypto.getRandomValues(new Uint32Array(1))[0] % s.players.length;
    s.passes = 0;
    s.result = null;
    s.phase = "playing";
    s.log = [`Game started. ${s.players[s.turn].name} goes first.`];
  }

  reveal() {
    const s = this.s;
    const totals = s.players.map((p) => sum([p.hidden, ...p.visible]));
    const [a, b] = totals;
    if (a === b) return this.finish(null, `Both players have ${a}. It's a draw!`);
    const winner = a > b ? 0 : 1;
    this.finish(s.players[winner].id, `${s.players[winner].name} wins with ${totals[winner]} vs ${totals[1 - winner]}.`);
  }

  finish(winnerId, reason) {
    const s = this.s;
    s.phase = "finished";
    s.result = { winnerId, reason };
    s.log.push(reason);
  }

  viewFor(cid) {
    const s = this.s;
    const revealed = s.phase === "finished";
    return {
      type: "state",
      you: cid,
      phase: s.phase,
      hostId: s.hostId,
      turnId: s.phase === "playing" ? s.players[s.turn]?.id : null,
      passes: s.passes,
      last: s.last ?? null,
      deckCount: s.deck.length,
      result: s.result,
      log: s.log.slice(-6),
      players: s.players.map((p) => {
        const inPlay = s.phase !== "lobby";
        const canSee = revealed || p.id === cid;
        return {
          id: p.id,
          name: p.name,
          connected: this.isConnected(p.id),
          visible: p.visible,
          hasHidden: inPlay,
          hidden: inPlay && canSee ? p.hidden : null,
          total: inPlay ? sum(canSee ? [p.hidden, ...p.visible] : p.visible) : 0,
        };
      }),
    };
  }

  broadcast() {
    if (!this.s) return;
    for (const ws of this.ctx.getWebSockets()) {
      const cid = ws.deserializeAttachment()?.cid;
      if (!cid || ws.readyState !== 1) continue;
      try {
        ws.send(JSON.stringify(this.viewFor(cid)));
      } catch {}
    }
  }
}
