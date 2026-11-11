// Chainline lobby link: players' browsers talk directly to each other (WebRTC data channels).
// The host's browser is the hub; everyone else connects to it by lobby code. Only the first handshake goes
// through a public meeting point (the PeerJS project's free broker); game messages go player to player.
// The game gets no callbacks: it asks poll() once a frame and receives everything that arrived since.
// Everything a peer sends is treated as untrusted data: size-limited, rate-limited, never executed.
window.ChainNet = (function () {
  const PREFIX = "chainline-v1-", MAX_PEERS = 7, MAX_LEN = 600, MAX_PER_SEC = 90, MAX_INBOX = 600;
  let peer = null, conns = {}, inbox = [], state = "idle", isHost = false, me = "", hostId = "", err = "";
  const push = (m) => { if (inbox.length < MAX_INBOX) inbox.push(m); };
  function fail(e) { err = (e && e.type) || String(e); push({ t: "error", e: err }); if (state !== "hosting" && state !== "joined") state = "error"; }
  function okMsg(c, d) {
    if (d === null || typeof d !== "object") return false;
    const now = Date.now();
    if (now - c._t0 > 1000) { c._t0 = now; c._n = 0; }
    if (++c._n > MAX_PER_SEC) return false;
    try { return JSON.stringify(d).length <= MAX_LEN; } catch (x) { return false; }
  }
  function wire(c) {
    c._t0 = 0; c._n = 0;
    c.on("open", () => {
      if (isHost && Object.keys(conns).length >= MAX_PEERS) { c.close(); return; }
      conns[c.peer] = c; if (!isHost) state = "joined"; push({ t: "open", id: c.peer });
    });
    c.on("data", (d) => {
      if (!okMsg(c, d)) return;
      if (isHost) {
        // a joiner speaks only for itself: the sender id is the connection's, never what the message claims
        push({ t: "msg", id: c.peer, d: d });
        for (const k in conns) if (k !== c.peer) conns[k].send({ from: c.peer, d: d });
      } else if (typeof d.from === "string" && d.d !== null && typeof d.d === "object") {
        push({ t: "msg", id: d.from, d: d.d });
      }
    });
    c.on("close", () => { if (conns[c.peer] === c) { delete conns[c.peer]; push({ t: "close", id: c.peer }); } if (!isHost) state = "closed"; });
    c.on("error", fail);
  }
  return {
    host(code) {
      this.leave(); isHost = true; state = "opening"; err = ""; hostId = PREFIX + code;
      peer = new Peer(hostId);
      peer.on("open", (id) => { me = id; state = "hosting"; push({ t: "ready" }); });
      peer.on("connection", wire);
      peer.on("error", fail);
    },
    join(code) {
      this.leave(); isHost = false; state = "opening"; err = ""; hostId = PREFIX + code;
      peer = new Peer();
      peer.on("open", (id) => { me = id; state = "connecting"; wire(peer.connect(hostId, { reliable: true })); });
      peer.on("error", fail);
    },
    send(obj) { for (const k in conns) conns[k].send(isHost ? { from: me, d: obj } : obj); },
    kick(id) { if (conns[id]) conns[id].close(); },
    info() { return JSON.stringify({ state, host: hostId, n: Object.keys(conns).length, err }); },   // for tests: does not empty the inbox
    poll() { const out = JSON.stringify({ state, me, host: isHost, n: Object.keys(conns).length, err, inbox }); inbox = []; return out; },
    leave() { if (peer) { try { peer.destroy(); } catch (x) {} } peer = null; conns = {}; inbox = []; state = "idle"; me = ""; },
  };
})();
