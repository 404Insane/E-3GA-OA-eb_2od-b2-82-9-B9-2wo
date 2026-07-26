// Discord Online + Voice Keeper
//
// Install deps (once):  npm install ws dotenv
//
// Create a .env file:
//   DISCORD_TOKEN=your_token_here
//   VOICE_CHANNEL_ID=channel_id_to_sit_in
//   GUILD_ID=your_server_id
//   HOURS=7
//
// Run:  node index.js
//
// Commands (works in any channel or DM):
//   ,start    — join voice and begin farming
//   ,endfarm  — stop farming and leave voice

require("dotenv").config();
const WebSocket = require("ws");

const TOKEN         = process.env.DISCORD_TOKEN;
const VOICE_CHANNEL = process.env.VOICE_CHANNEL_ID || "1530928969120092203";
const GUILD_ID      = process.env.GUILD_ID         || "1530928968512045086";
const FARM_HOURS    = parseFloat(process.env.HOURS || "12");

// Channel that receives the "farm completed" message
const REPORT_CHANNEL = "1531027000851042414";
const REPORT_GUILD   = "1531026785209422104";

if (!TOKEN) {
  console.error("DISCORD_TOKEN is not set. Add it to a .env file.");
  process.exit(1);
}

// ── constants ──────────────────────────────────────────────────────
const GW  = "wss://gateway.discord.gg/?v=10&encoding=json";
const API = "https://discord.com/api/v10";
const UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
const WS_HEADERS = { Origin: "https://discord.com", "User-Agent": UA };

// ── helpers ────────────────────────────────────────────────────────
const rand    = (lo, hi) => lo + Math.random() * (hi - lo);
const sleep   = ms => new Promise(r => setTimeout(r, ms));

function fmtHours(ms) {
  const totalMin = Math.floor(ms / 60_000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  if (h === 0) return `${m}m`;
  if (m === 0) return `${h}h`;
  return `${h}h ${m}m`;
}

// ── discord REST ───────────────────────────────────────────────────
const REST_HEADERS = {
  Authorization:  TOKEN,
  "Content-Type": "application/json",
  "User-Agent":   UA,
};

async function apiPost(path, body = {}) {
  try {
    await fetch(API + path, {
      method:  "POST",
      headers: REST_HEADERS,
      body:    JSON.stringify(body),
    });
  } catch {}
}

async function sendTyping(channelId) {
  await apiPost(`/channels/${channelId}/typing`);
}

async function sendMessage(channelId, content) {
  await apiPost(`/channels/${channelId}/messages`, { content });
}

// Simulates a human typing and sending — call AFTER the initial delay
async function humanSend(channelId, content) {
  await sendTyping(channelId);
  // hold typing for a realistic duration based on message length
  const typingMs = Math.min(content.length * rand(40, 80), 8000);
  await sleep(typingMs);
  await sendMessage(channelId, content);
}

// ── farm state ─────────────────────────────────────────────────────
const farm = {
  active:    false,
  startedAt: null,
  endsAt:    null,
  timer:     null,
};

// forward ref — assigned after Gateway is instantiated
let gw;

async function startFarm(commandChannelId) {
  if (farm.active) return; // already running, ignore

  farm.active    = true;
  farm.startedAt = Date.now();
  farm.endsAt    = farm.startedAt + FARM_HOURS * 3_600_000;

  gw.joinVoice();

  const voiceLink = `https://discord.com/channels/${GUILD_ID}/${VOICE_CHANNEL}`;
  const endTs     = Math.floor(farm.endsAt / 1000);
  const content   = `🟢: joined ${voiceLink} | time left until vc farm ends: <t:${endTs}:R>`;

  await humanSend(commandChannelId, content);
  console.log("farm started  —  ends in " + FARM_HOURS + "h");

  // auto-complete when timer expires
  farm.timer = setTimeout(() => completeFarm(), FARM_HOURS * 3_600_000);
}

async function stopFarm(commandChannelId) {
  if (!farm.active) return;

  const elapsed = Date.now() - farm.startedAt;
  clearTimeout(farm.timer);
  farm.active = false;

  gw.leaveVoice();

  const content = `🔴: stopped vc farming, total hours farmed: ${fmtHours(elapsed)}`;
  await humanSend(commandChannelId, content);
  console.log("farm stopped  —  farmed " + fmtHours(elapsed));
}

async function completeFarm() {
  if (!farm.active) return;

  const elapsed = Date.now() - farm.startedAt;
  farm.active   = false;

  gw.leaveVoice();

  const voiceLink = `https://discord.com/channels/${GUILD_ID}/${VOICE_CHANNEL}`;
  const content   = `🤑: voice chat farm for ${voiceLink} completed, total vc hrs farmed: ${fmtHours(elapsed)}`;
  await humanSend(REPORT_CHANNEL, content);
  console.log("farm completed  —  farmed " + fmtHours(elapsed));
}

// ── voice gateway ──────────────────────────────────────────────────
class VoiceGateway {
  constructor(endpoint, serverId, userId, sessionId, token) {
    this._url       = "wss://" + endpoint.replace(/:80$/, "") + "/?v=8";
    this._serverId  = serverId;
    this._userId    = userId;
    this._sessionId = sessionId;
    this._token     = token;
    this._ws        = null;
    this._timer     = null;
    this._nonce     = 0;
    this._dead      = false;
  }

  open() {
    this._ws = new WebSocket(this._url, { headers: WS_HEADERS });

    this._ws.on("message", raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }

      // Hello — start heartbeat then identify
      if (msg.op === 8) {
        clearInterval(this._timer);
        const ms = msg.d.heartbeat_interval * rand(0.97, 1.03);
        this._timer = setInterval(() => {
          if (this._ws?.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify({ op: 3, d: ++this._nonce }));
        }, ms);

        this._ws.send(JSON.stringify({
          op: 0,
          d: {
            server_id:  this._serverId,
            user_id:    this._userId,
            session_id: this._sessionId,
            token:      this._token,
          },
        }));
      }

      // Ready — complete handshake without transmitting audio
      if (msg.op === 2) {
        this._ws.send(JSON.stringify({
          op: 1,
          d: {
            protocol: "udp",
            data: { address: "0.0.0.0", port: 0, mode: "xsalsa20_poly1305" },
          },
        }));
        // Mark as present but silent
        setTimeout(() => {
          if (this._ws?.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify({ op: 5, d: { speaking: 0, delay: 0, ssrc: msg.d.ssrc } }));
        }, rand(600, 1800));
      }
    });

    this._ws.on("close", () => { if (!this._dead) clearInterval(this._timer); });
    this._ws.on("error", () => {});
  }

  close() {
    this._dead = true;
    clearInterval(this._timer);
    try { this._ws?.close(); } catch {}
  }
}

// ── main gateway ───────────────────────────────────────────────────
class Gateway {
  constructor(token) {
    this._token        = token;
    this._ws           = null;
    this._hbTimer      = null;
    this._hbAck        = true;
    this._seq          = null;
    this._sessionId    = null;
    this._resumeGw     = null;
    this._userId       = null;
    this._voice        = null;
    this._voiceSession = null;
    this._voiceServer  = null;
    this._stopped      = false;
    this._pendingJoin  = false; // waiting for voice server info
  }

  _send(p) {
    try {
      if (this._ws?.readyState === WebSocket.OPEN)
        this._ws.send(JSON.stringify(p));
    } catch {}
  }

  // ── heartbeat ────────────────────────────────────────────────────
  _startHeartbeat(ms) {
    clearInterval(this._hbTimer);
    // Discord spec: jitter the first beat by a random fraction
    setTimeout(() => {
      if (this._stopped) return;
      this._beat();
      this._hbTimer = setInterval(() => this._beat(), ms * rand(0.98, 1.02));
    }, ms * Math.random());
  }

  _beat() {
    if (!this._hbAck) { this._reconnect(false); return; }
    this._hbAck = false;
    this._send({ op: 1, d: this._seq });
  }

  // ── auth ──────────────────────────────────────────────────────────
  _identify() {
    this._send({
      op: 2,
      d: {
        token:        this._token,
        capabilities: 16381,
        properties: {
          os:                       "Windows",
          browser:                  "Chrome",
          device:                   "",
          system_locale:            "en-US",
          browser_user_agent:       UA,
          browser_version:          "125.0.0.0",
          os_version:               "10",
          referrer:                 "https://discord.com/channels/@me",
          referring_domain:         "discord.com",
          referrer_current:         "",
          referring_domain_current: "",
          release_channel:          "stable",
          client_build_number:      306855,
          client_event_source:      null,
        },
        presence: { status: "online", since: 0, activities: [], afk: false },
        compress: false,
        client_state: {
          guild_versions:               {},
          highest_last_message_id:      "0",
          read_state_version:           0,
          user_guild_settings_version:  -1,
          user_settings_version:        -1,
          private_channels_version:     "0",
          api_code_version:             0,
        },
      },
    });
  }

  _resume() {
    this._send({
      op: 6,
      d: { token: this._token, session_id: this._sessionId, seq: this._seq },
    });
  }

  // ── voice control (public) ────────────────────────────────────────
  joinVoice() {
    if (!VOICE_CHANNEL || !GUILD_ID) return;
    this._pendingJoin = true;
    this._send({
      op: 4,
      d: {
        guild_id:   GUILD_ID,
        channel_id: VOICE_CHANNEL,
        self_mute:  false,
        self_deaf:  false,
        self_video: false,
      },
    });
  }

  leaveVoice() {
    this._pendingJoin = false;
    this._voice?.close();
    this._voice = null;
    this._voiceSession = null;
    this._voiceServer  = null;
    // Disconnect from voice server
    this._send({
      op: 4,
      d: { guild_id: GUILD_ID, channel_id: null, self_mute: false, self_deaf: false },
    });
  }

  // ── internal voice setup ──────────────────────────────────────────
  _tryOpenVoice() {
    if (!this._pendingJoin)    return;
    if (!this._voiceSession)   return;
    if (!this._voiceServer)    return;

    this._voice?.close();
    this._voice = new VoiceGateway(
      this._voiceServer.endpoint,
      GUILD_ID,
      this._userId,
      this._voiceSession,
      this._voiceServer.token,
    );
    this._voice.open();
    this._voiceSession = null;
    this._voiceServer  = null;
  }

  // ── dispatch ──────────────────────────────────────────────────────
  _dispatch(t, d) {
    if (t === "READY") {
      this._userId    = d.user.id;
      this._sessionId = d.session_id;
      this._resumeGw  = d.resume_gateway_url;
      const tag = d.user.username + (d.user.discriminator && d.user.discriminator !== "0" ? "#" + d.user.discriminator : "");
      console.log("online  (" + tag + ")  —  waiting for ,start command");
    }

    if (t === "RESUMED") {
      console.log("session resumed");
      // re-join voice if farm is still active
      if (farm.active) {
        setTimeout(() => this.joinVoice(), rand(1000, 3000));
      }
    }

    if (t === "VOICE_STATE_UPDATE" && d.user_id === this._userId) {
      this._voiceSession = d.session_id;
      this._tryOpenVoice();
    }

    if (t === "VOICE_SERVER_UPDATE") {
      this._voiceServer = { token: d.token, endpoint: d.endpoint };
      this._tryOpenVoice();
    }

    if (t === "MESSAGE_CREATE") {
      const content = (d.content || "").trim().toLowerCase();
      const cid     = d.channel_id;

      if (content === ",start") {
        const delay = Math.floor(rand(5_000, 34_000));
        console.log(",start received  —  joining in " + Math.round(delay / 1000) + "s");
        setTimeout(() => startFarm(cid), delay);
      }

      if (content === ",endfarm") {
        if (!farm.active) return;
        const delay = Math.floor(rand(5_000, 34_000));
        console.log(",endfarm received  —  stopping in " + Math.round(delay / 1000) + "s");
        setTimeout(() => stopFarm(cid), delay);
      }
    }
  }

  // ── connection lifecycle ──────────────────────────────────────────
  _reconnect(canResume) {
    clearInterval(this._hbTimer);
    try { this._ws?.close(1000); } catch {}
    const delay = rand(4_000, 9_000);
    setTimeout(() => this._connect(canResume), delay);
  }

  _connect(resume) {
    const url = resume && this._resumeGw
      ? this._resumeGw + "?v=10&encoding=json"
      : GW;

    this._ws = new WebSocket(url, { headers: WS_HEADERS });

    this._ws.on("open", () => {});

    this._ws.on("message", raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const op = msg.op;
      if (msg.s != null) this._seq = msg.s;

      if (op === 10) {
        this._startHeartbeat(msg.d.heartbeat_interval);
        resume && this._sessionId ? this._resume() : this._identify();
      }
      if (op === 11) { this._hbAck = true; }
      if (op === 0)  { this._dispatch(msg.t, msg.d); }
      if (op === 7)  { this._reconnect(true); }
      if (op === 9)  { setTimeout(() => this._reconnect(msg.d === true), rand(1_000, 5_000)); }
    });

    this._ws.on("close", code => {
      clearInterval(this._hbTimer);
      if (this._stopped) return;
      const fatal = [4004, 4010, 4011, 4013, 4014].includes(code);
      if (fatal) { console.error("fatal close " + code + "  — check your token"); return; }
      this._reconnect(true);
    });

    this._ws.on("error", () => {});
  }

  start() { this._connect(false); }

  stop() {
    this._stopped = true;
    clearInterval(this._hbTimer);
    this._voice?.close();
    try { this._ws?.close(1000); } catch {}
  }
}

// ── run ────────────────────────────────────────────────────────────
gw = new Gateway(TOKEN);
gw.start();

process.on("SIGINT", async () => {
  console.log("\nstopping...");
  if (farm.active) {
    const elapsed = Date.now() - farm.startedAt;
    clearTimeout(farm.timer);
    gw.leaveVoice();
    console.log("farmed " + fmtHours(elapsed) + " before exit");
  }
  gw.stop();
  process.exit(0);
});
// Discord Online + Voice Keeper
//
// Install deps (once):  npm install ws dotenv
//
// Create a .env file:
//   DISCORD_TOKEN=your_token_here
//   VOICE_CHANNEL_ID=channel_id_to_sit_in
//   GUILD_ID=your_server_id
//   HOURS=7
//
// Run:  node index.js
//
// Commands (works in any channel or DM):
//   ,start    — join voice and begin farming
//   ,endfarm  — stop farming and leave voice

require("dotenv").config();
const WebSocket = require("ws");

const TOKEN         = process.env.DISCORD_TOKEN;
const VOICE_CHANNEL = process.env.VOICE_CHANNEL_ID || "1530928969120092203";
const GUILD_ID      = process.env.GUILD_ID         || "1530928968512045086";
const FARM_HOURS    = parseFloat(process.env.HOURS || "12");

// Channel that receives the "farm completed" message
const REPORT_CHANNEL = "1531027000851042414";
const REPORT_GUILD   = "1531026785209422104";

if (!TOKEN) {
  console.error("DISCORD_TOKEN is not set. Add it to a .env file.");
  process.exit(1);
}

// ── constants ──────────────────────────────────────────────────────
const GW  = "wss://gateway.discord.gg/?v=10&encoding=json";
const API = "https://discord.com/api/v10";
const UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
const WS_HEADERS = { Origin: "https://discord.com", "User-Agent": UA };

// ── helpers ────────────────────────────────────────────────────────
const rand    = (lo, hi) => lo + Math.random() * (hi - lo);
const sleep   = ms => new Promise(r => setTimeout(r, ms));

function fmtHours(ms) {
  const totalMin = Math.floor(ms / 60_000);
  const h = Math.floor(totalMin / 60);
  const m = totalMin % 60;
  if (h === 0) return `${m}m`;
  if (m === 0) return `${h}h`;
  return `${h}h ${m}m`;
}

// ── discord REST ───────────────────────────────────────────────────
const REST_HEADERS = {
  Authorization:  TOKEN,
  "Content-Type": "application/json",
  "User-Agent":   UA,
};

async function apiPost(path, body = {}) {
  try {
    await fetch(API + path, {
      method:  "POST",
      headers: REST_HEADERS,
      body:    JSON.stringify(body),
    });
  } catch {}
}

async function sendTyping(channelId) {
  await apiPost(`/channels/${channelId}/typing`);
}

async function sendMessage(channelId, content) {
  await apiPost(`/channels/${channelId}/messages`, { content });
}

// Simulates a human typing and sending — call AFTER the initial delay
async function humanSend(channelId, content) {
  await sendTyping(channelId);
  // hold typing for a realistic duration based on message length
  const typingMs = Math.min(content.length * rand(40, 80), 8000);
  await sleep(typingMs);
  await sendMessage(channelId, content);
}

// ── farm state ─────────────────────────────────────────────────────
const farm = {
  active:    false,
  startedAt: null,
  endsAt:    null,
  timer:     null,
};

// forward ref — assigned after Gateway is instantiated
let gw;

async function startFarm(commandChannelId) {
  if (farm.active) return; // already running, ignore

  farm.active    = true;
  farm.startedAt = Date.now();
  farm.endsAt    = farm.startedAt + FARM_HOURS * 3_600_000;

  gw.joinVoice();

  const voiceLink = `https://discord.com/channels/${GUILD_ID}/${VOICE_CHANNEL}`;
  const endTs     = Math.floor(farm.endsAt / 1000);
  const content   = `🟢: joined ${voiceLink} | time left until vc farm ends: <t:${endTs}:R>`;

  await humanSend(commandChannelId, content);
  console.log("farm started  —  ends in " + FARM_HOURS + "h");

  // auto-complete when timer expires
  farm.timer = setTimeout(() => completeFarm(), FARM_HOURS * 3_600_000);
}

async function stopFarm(commandChannelId) {
  if (!farm.active) return;

  const elapsed = Date.now() - farm.startedAt;
  clearTimeout(farm.timer);
  farm.active = false;

  gw.leaveVoice();

  const content = `🔴: stopped vc farming, total hours farmed: ${fmtHours(elapsed)}`;
  await humanSend(commandChannelId, content);
  console.log("farm stopped  —  farmed " + fmtHours(elapsed));
}

async function completeFarm() {
  if (!farm.active) return;

  const elapsed = Date.now() - farm.startedAt;
  farm.active   = false;

  gw.leaveVoice();

  const voiceLink = `https://discord.com/channels/${GUILD_ID}/${VOICE_CHANNEL}`;
  const content   = `🤑: voice chat farm for ${voiceLink} completed, total vc hrs farmed: ${fmtHours(elapsed)}`;
  await humanSend(REPORT_CHANNEL, content);
  console.log("farm completed  —  farmed " + fmtHours(elapsed));
}

// ── voice gateway ──────────────────────────────────────────────────
class VoiceGateway {
  constructor(endpoint, serverId, userId, sessionId, token) {
    this._url       = "wss://" + endpoint.replace(/:80$/, "") + "/?v=8";
    this._serverId  = serverId;
    this._userId    = userId;
    this._sessionId = sessionId;
    this._token     = token;
    this._ws        = null;
    this._timer     = null;
    this._nonce     = 0;
    this._dead      = false;
  }

  open() {
    this._ws = new WebSocket(this._url, { headers: WS_HEADERS });

    this._ws.on("message", raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }

      // Hello — start heartbeat then identify
      if (msg.op === 8) {
        clearInterval(this._timer);
        const ms = msg.d.heartbeat_interval * rand(0.97, 1.03);
        this._timer = setInterval(() => {
          if (this._ws?.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify({ op: 3, d: ++this._nonce }));
        }, ms);

        this._ws.send(JSON.stringify({
          op: 0,
          d: {
            server_id:  this._serverId,
            user_id:    this._userId,
            session_id: this._sessionId,
            token:      this._token,
          },
        }));
      }

      // Ready — complete handshake without transmitting audio
      if (msg.op === 2) {
        this._ws.send(JSON.stringify({
          op: 1,
          d: {
            protocol: "udp",
            data: { address: "0.0.0.0", port: 0, mode: "xsalsa20_poly1305" },
          },
        }));
        // Mark as present but silent
        setTimeout(() => {
          if (this._ws?.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify({ op: 5, d: { speaking: 0, delay: 0, ssrc: msg.d.ssrc } }));
        }, rand(600, 1800));
      }
    });

    this._ws.on("close", () => { if (!this._dead) clearInterval(this._timer); });
    this._ws.on("error", () => {});
  }

  close() {
    this._dead = true;
    clearInterval(this._timer);
    try { this._ws?.close(); } catch {}
  }
}

// ── main gateway ───────────────────────────────────────────────────
class Gateway {
  constructor(token) {
    this._token        = token;
    this._ws           = null;
    this._hbTimer      = null;
    this._hbAck        = true;
    this._seq          = null;
    this._sessionId    = null;
    this._resumeGw     = null;
    this._userId       = null;
    this._voice        = null;
    this._voiceSession = null;
    this._voiceServer  = null;
    this._stopped      = false;
    this._pendingJoin  = false; // waiting for voice server info
  }

  _send(p) {
    try {
      if (this._ws?.readyState === WebSocket.OPEN)
        this._ws.send(JSON.stringify(p));
    } catch {}
  }

  // ── heartbeat ────────────────────────────────────────────────────
  _startHeartbeat(ms) {
    clearInterval(this._hbTimer);
    // Discord spec: jitter the first beat by a random fraction
    setTimeout(() => {
      if (this._stopped) return;
      this._beat();
      this._hbTimer = setInterval(() => this._beat(), ms * rand(0.98, 1.02));
    }, ms * Math.random());
  }

  _beat() {
    if (!this._hbAck) { this._reconnect(false); return; }
    this._hbAck = false;
    this._send({ op: 1, d: this._seq });
  }

  // ── auth ──────────────────────────────────────────────────────────
  _identify() {
    this._send({
      op: 2,
      d: {
        token:        this._token,
        capabilities: 16381,
        properties: {
          os:                       "Windows",
          browser:                  "Chrome",
          device:                   "",
          system_locale:            "en-US",
          browser_user_agent:       UA,
          browser_version:          "125.0.0.0",
          os_version:               "10",
          referrer:                 "https://discord.com/channels/@me",
          referring_domain:         "discord.com",
          referrer_current:         "",
          referring_domain_current: "",
          release_channel:          "stable",
          client_build_number:      306855,
          client_event_source:      null,
        },
        presence: { status: "online", since: 0, activities: [], afk: false },
        compress: false,
        client_state: {
          guild_versions:               {},
          highest_last_message_id:      "0",
          read_state_version:           0,
          user_guild_settings_version:  -1,
          user_settings_version:        -1,
          private_channels_version:     "0",
          api_code_version:             0,
        },
      },
    });
  }

  _resume() {
    this._send({
      op: 6,
      d: { token: this._token, session_id: this._sessionId, seq: this._seq },
    });
  }

  // ── voice control (public) ────────────────────────────────────────
  joinVoice() {
    if (!VOICE_CHANNEL || !GUILD_ID) return;
    this._pendingJoin = true;
    this._send({
      op: 4,
      d: {
        guild_id:   GUILD_ID,
        channel_id: VOICE_CHANNEL,
        self_mute:  false,
        self_deaf:  false,
        self_video: false,
      },
    });
  }

  leaveVoice() {
    this._pendingJoin = false;
    this._voice?.close();
    this._voice = null;
    this._voiceSession = null;
    this._voiceServer  = null;
    // Disconnect from voice server
    this._send({
      op: 4,
      d: { guild_id: GUILD_ID, channel_id: null, self_mute: false, self_deaf: false },
    });
  }

  // ── internal voice setup ──────────────────────────────────────────
  _tryOpenVoice() {
    if (!this._pendingJoin)    return;
    if (!this._voiceSession)   return;
    if (!this._voiceServer)    return;

    this._voice?.close();
    this._voice = new VoiceGateway(
      this._voiceServer.endpoint,
      GUILD_ID,
      this._userId,
      this._voiceSession,
      this._voiceServer.token,
    );
    this._voice.open();
    this._voiceSession = null;
    this._voiceServer  = null;
  }

  // ── dispatch ──────────────────────────────────────────────────────
  _dispatch(t, d) {
    if (t === "READY") {
      this._userId    = d.user.id;
      this._sessionId = d.session_id;
      this._resumeGw  = d.resume_gateway_url;
      const tag = d.user.username + (d.user.discriminator && d.user.discriminator !== "0" ? "#" + d.user.discriminator : "");
      console.log("online  (" + tag + ")  —  waiting for ,start command");
    }

    if (t === "RESUMED") {
      console.log("session resumed");
      // re-join voice if farm is still active
      if (farm.active) {
        setTimeout(() => this.joinVoice(), rand(1000, 3000));
      }
    }

    if (t === "VOICE_STATE_UPDATE" && d.user_id === this._userId) {
      this._voiceSession = d.session_id;
      this._tryOpenVoice();
    }

    if (t === "VOICE_SERVER_UPDATE") {
      this._voiceServer = { token: d.token, endpoint: d.endpoint };
      this._tryOpenVoice();
    }

    if (t === "MESSAGE_CREATE") {
      const content = (d.content || "").trim().toLowerCase();
      const cid     = d.channel_id;

      if (content === ",start") {
        const delay = Math.floor(rand(5_000, 34_000));
        console.log(",start received  —  joining in " + Math.round(delay / 1000) + "s");
        setTimeout(() => startFarm(cid), delay);
      }

      if (content === ",endfarm") {
        if (!farm.active) return;
        const delay = Math.floor(rand(5_000, 34_000));
        console.log(",endfarm received  —  stopping in " + Math.round(delay / 1000) + "s");
        setTimeout(() => stopFarm(cid), delay);
      }
    }
  }

  // ── connection lifecycle ──────────────────────────────────────────
  _reconnect(canResume) {
    clearInterval(this._hbTimer);
    try { this._ws?.close(1000); } catch {}
    const delay = rand(4_000, 9_000);
    setTimeout(() => this._connect(canResume), delay);
  }

  _connect(resume) {
    const url = resume && this._resumeGw
      ? this._resumeGw + "?v=10&encoding=json"
      : GW;

    this._ws = new WebSocket(url, { headers: WS_HEADERS });

    this._ws.on("open", () => {});

    this._ws.on("message", raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const op = msg.op;
      if (msg.s != null) this._seq = msg.s;

      if (op === 10) {
        this._startHeartbeat(msg.d.heartbeat_interval);
        resume && this._sessionId ? this._resume() : this._identify();
      }
      if (op === 11) { this._hbAck = true; }
      if (op === 0)  { this._dispatch(msg.t, msg.d); }
      if (op === 7)  { this._reconnect(true); }
      if (op === 9)  { setTimeout(() => this._reconnect(msg.d === true), rand(1_000, 5_000)); }
    });

    this._ws.on("close", code => {
      clearInterval(this._hbTimer);
      if (this._stopped) return;
      const fatal = [4004, 4010, 4011, 4013, 4014].includes(code);
      if (fatal) { console.error("fatal close " + code + "  — check your token"); return; }
      this._reconnect(true);
    });

    this._ws.on("error", () => {});
  }

  start() { this._connect(false); }

  stop() {
    this._stopped = true;
    clearInterval(this._hbTimer);
    this._voice?.close();
    try { this._ws?.close(1000); } catch {}
  }
}

// ── run ────────────────────────────────────────────────────────────
gw = new Gateway(TOKEN);
gw.start();

process.on("SIGINT", async () => {
  console.log("\nstopping...");
  if (farm.active) {
    const elapsed = Date.now() - farm.startedAt;
    clearTimeout(farm.timer);
    gw.leaveVoice();
    console.log("farmed " + fmtHours(elapsed) + " before exit");
  }
  gw.stop();
  process.exit(0);
});

