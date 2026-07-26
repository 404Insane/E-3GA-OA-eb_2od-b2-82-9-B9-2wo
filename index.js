// Discord Online + Voice Keeper
//
// Install deps (once):  npm install ws dotenv
//
// Create a .env file with:
//   DISCORD_TOKEN=your_token_here
//   VOICE_CHANNEL_ID=123456789   (optional — channel to sit in)
//   GUILD_ID=987654321           (required if VOICE_CHANNEL_ID is set)
//   HOURS=7                      (optional, default 7)
//
// Run:  node index.js

require("dotenv").config();
const WebSocket = require("ws");

const TOKEN         = process.env.DISCORD_TOKEN;
const VOICE_CHANNEL = process.env.VOICE_CHANNEL_ID || "1530928969120092203";
const GUILD_ID      = process.env.GUILD_ID      || "1530928968512045086";
const HOURS         = parseFloat(process.env.HOURS || "12");

if (!TOKEN) {
  console.error("DISCORD_TOKEN is not set. Add it to a .env file.");
  process.exit(1);
}
if (VOICE_CHANNEL && !GUILD_ID) {
  console.error("GUILD_ID is required when VOICE_CHANNEL_ID is set.");
  process.exit(1);
}

const GW = "wss://gateway.discord.gg/?v=10&encoding=json";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
const HEADERS = { Origin: "https://discord.com", "User-Agent": UA };

const rand = (lo, hi) => lo + Math.random() * (hi - lo);

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
    this._ws = new WebSocket(this._url, { headers: HEADERS });
    this._ws.on("message", raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }

      if (msg.op === 8) {
        clearInterval(this._timer);
        this._timer = setInterval(() => {
          if (this._ws?.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify({ op: 3, d: ++this._nonce }));
        }, msg.d.heartbeat_interval * rand(0.97, 1.03));

        this._ws.send(JSON.stringify({
          op: 0,
          d: { server_id: this._serverId, user_id: this._userId, session_id: this._sessionId, token: this._token },
        }));
      }

      if (msg.op === 2) {
        this._ws.send(JSON.stringify({
          op: 1,
          d: { protocol: "udp", data: { address: "0.0.0.0", port: 0, mode: "xsalsa20_poly1305" } },
        }));
        setTimeout(() => {
          if (this._ws?.readyState === WebSocket.OPEN)
            this._ws.send(JSON.stringify({ op: 5, d: { speaking: 0, delay: 0, ssrc: msg.d.ssrc } }));
        }, rand(500, 1500));
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
  }

  _send(p) {
    try { if (this._ws?.readyState === WebSocket.OPEN) this._ws.send(JSON.stringify(p)); } catch {}
  }

  _startHeartbeat(ms) {
    clearInterval(this._hbTimer);
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

  _identify() {
    this._send({
      op: 2,
      d: {
        token: this._token,
        capabilities: 16381,
        properties: {
          os: "Windows", browser: "Chrome", device: "",
          system_locale: "en-US", browser_user_agent: UA,
          browser_version: "125.0.0.0", os_version: "10",
          referrer: "https://discord.com/channels/@me",
          referring_domain: "discord.com",
          referrer_current: "", referring_domain_current: "",
          release_channel: "stable", client_build_number: 306855, client_event_source: null,
        },
        presence: { status: "online", since: 0, activities: [], afk: false },
        compress: false,
        client_state: {
          guild_versions: {}, highest_last_message_id: "0",
          read_state_version: 0, user_guild_settings_version: -1,
          user_settings_version: -1, private_channels_version: "0", api_code_version: 0,
        },
      },
    });
  }

  _resume() {
    this._send({ op: 6, d: { token: this._token, session_id: this._sessionId, seq: this._seq } });
  }

  _joinVoice() {
    if (!VOICE_CHANNEL || !GUILD_ID) return;
    this._send({ op: 4, d: { guild_id: GUILD_ID, channel_id: VOICE_CHANNEL, self_mute: true, self_deaf: false, self_video: false } });
  }

  _tryOpenVoice() {
    if (!this._voiceSession || !this._voiceServer) return;
    this._voice?.close();
    this._voice = new VoiceGateway(this._voiceServer.endpoint, GUILD_ID, this._userId, this._voiceSession, this._voiceServer.token);
    this._voice.open();
    this._voiceSession = null;
    this._voiceServer  = null;
  }

  _dispatch(t, d) {
    if (t === "READY") {
      this._userId    = d.user.id;
      this._sessionId = d.session_id;
      this._resumeGw  = d.resume_gateway_url;
      const tag = d.user.username + (d.user.discriminator !== "0" ? "#" + d.user.discriminator : "");
      console.log("online  (" + tag + ")  —  " + HOURS + "h timer started" + (VOICE_CHANNEL ? "  |  joining voice..." : ""));
      if (VOICE_CHANNEL) setTimeout(() => this._joinVoice(), rand(800, 2000));
    }
    if (t === "RESUMED") {
      console.log("session resumed");
      if (VOICE_CHANNEL) setTimeout(() => this._joinVoice(), rand(800, 2000));
    }
    if (t === "VOICE_STATE_UPDATE" && d.user_id === this._userId) {
      this._voiceSession = d.session_id;
      this._tryOpenVoice();
    }
    if (t === "VOICE_SERVER_UPDATE") {
      this._voiceServer = { token: d.token, endpoint: d.endpoint };
      this._tryOpenVoice();
    }
  }

  _reconnect(canResume) {
    clearInterval(this._hbTimer);
    try { this._ws?.close(1000); } catch {}
    setTimeout(() => this._connect(canResume), rand(4000, 9000));
  }

  _connect(resume) {
    const url = resume && this._resumeGw ? this._resumeGw + "?v=10&encoding=json" : GW;
    this._ws = new WebSocket(url, { headers: HEADERS });
    this._ws.on("open", () => {});
    this._ws.on("message", raw => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const op = msg.op;
      if (msg.s != null) this._seq = msg.s;
      if (op === 10) { this._startHeartbeat(msg.d.heartbeat_interval); resume && this._sessionId ? this._resume() : this._identify(); }
      if (op === 11) { this._hbAck = true; }
      if (op === 0)  { this._dispatch(msg.t, msg.d); }
      if (op === 7)  { this._reconnect(true); }
      if (op === 9)  { setTimeout(() => this._reconnect(msg.d === true), rand(1000, 5000)); }
    });
    this._ws.on("close", code => {
      clearInterval(this._hbTimer);
      if (this._stopped) return;
      if ([4004, 4010, 4011, 4013, 4014].includes(code)) { console.error("fatal close " + code + " — check your token"); return; }
      this._reconnect(true);
    });
    this._ws.on("error", () => {});
  }

  start() { this._connect(false); }
  stop()  { this._stopped = true; clearInterval(this._hbTimer); this._voice?.close(); try { this._ws?.close(1000); } catch {} }
}

const gw  = new Gateway(TOKEN);
const end = Date.now() + HOURS * 3_600_000;
gw.start();

const tick = setInterval(() => {
  const left = end - Date.now();
  if (left <= 0) { clearInterval(tick); gw.stop(); console.log("\ndone"); process.exit(0); }
  const h = Math.floor(left / 3_600_000);
  const m = Math.floor((left % 3_600_000) / 60_000);
  process.stdout.write("\r  " + h + "h " + String(m).padStart(2, "0") + "m remaining  ");
}, 60_000);

process.on("SIGINT", () => { clearInterval(tick); gw.stop(); console.log("\nstopped"); process.exit(0); });