import { GameRoom } from "./room.js";

export { GameRoom };

const CODE_RE = /^\d{6}$/;

function randomCode() {
  const n = crypto.getRandomValues(new Uint32Array(1))[0];
  return String(100000 + (n % 900000));
}

function roomStub(env, code) {
  return env.GAME_ROOM.get(env.GAME_ROOM.idFromName(code));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Host: allocate a unique 6-digit room code.
    if (url.pathname === "/api/host" && request.method === "POST") {
      for (let i = 0; i < 20; i++) {
        const code = randomCode();
        const res = await roomStub(env, code).fetch("https://room/init", { method: "POST" });
        if (res.ok) return Response.json({ code });
      }
      return Response.json({ error: "Could not allocate a room, try again." }, { status: 503 });
    }

    // Join: check that a room exists before opening the socket.
    const info = url.pathname.match(/^\/api\/room\/(\d{6})$/);
    if (info) {
      const res = await roomStub(env, info[1]).fetch("https://room/info");
      return new Response(res.body, { status: res.status, headers: { "content-type": "application/json" } });
    }

    // WebSocket to the room's Durable Object.
    if (url.pathname === "/api/ws") {
      const code = url.searchParams.get("code") || "";
      if (!CODE_RE.test(code)) return new Response("Bad room code", { status: 400 });
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      return roomStub(env, code).fetch(request);
    }

    return new Response("Not found", { status: 404 });
  },
};
