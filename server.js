/**
 * server.js — TikTok Bridge Server v2.0 (ESM)
 * ─────────────────────────────────────────────────────────────────────────────
 * يربط ألعاب HTML5 مع TikTok Live عبر Socket.IO.
 * يتعامل فقط مع TikTokDirectAdapter ويتصل مباشرة دون خدمة توقيع مدفوعة.
 *
 * Socket Events (قادمة من اللعبة):
 *   join_game  → { username }
 *   leave_game → { username }
 *   get_stats  → { username }
 *   get_gift_catalog → { username, force }
 *
 * Socket Events (صادرة إلى اللعبة) — محفوظة بالكامل:
 *   joined_game         → { username, profilePictureUrl }
 *   join_error          → { message }
 *   tiktok_like         → { streamer, liker, nickname, profilePictureUrl, likeCount, userTotalLikes, streamTotalLikes, timestamp }
 *   tiktok_comment      → { streamer, commenter, nickname, profilePictureUrl, comment, timestamp }
 *   tiktok_gift         → legacy fields + giftId, giftNameNormalized, totalDiamonds,
 *                          streak flags, eventId and schemaVersion
 *   tiktok_follow       → { streamer, follower, nickname, profilePictureUrl, timestamp }
 *   tiktok_share        → { streamer, username, nickname, profilePictureUrl, timestamp }
 *   join_progress       → { stage, username }
 *   tiktok_join         → { streamer, member, nickname, profilePictureUrl, timestamp }
 *   tiktok_disconnected → { username }
 *   tiktok_reconnected  → { username }
 *   tiktok_error        → { message }
 *   stats               → { username, stats, connected }
 * ─────────────────────────────────────────────────────────────────────────────
 */

import express          from 'express';
import { createServer } from 'http';
import { timingSafeEqual } from 'crypto';
import { Server }       from 'socket.io';
import { EventType }    from 'piratetok-live-js';
import TikTokAdapter    from './TikTokDirectAdapter.js';
import { installOverlaySessions } from './overlay-sessions.js';

/* ============================================================
   ALLOWED ORIGINS — المواقع المسموح لها فقط بالاتصال
============================================================ */

const TEST_ACCESS_CODE = String(process.env.TEST_ACCESS_CODE || '316349');

function isLoveligoOrigin(origin) {
    if (!origin || origin === 'null') return false;
    try {
        const url = new URL(origin);
        return url.protocol === 'https:'
            && (url.hostname === 'loveligo.com' || url.hostname.endsWith('.loveligo.com'));
    } catch {
        return false;
    }
}

function isOriginAllowed(origin) {
    return isLoveligoOrigin(origin);
}

function safeCodeEquals(candidate) {
    const supplied = Buffer.from(String(candidate || ''));
    const expected = Buffer.from(TEST_ACCESS_CODE);
    return supplied.length === expected.length && timingSafeEqual(supplied, expected);
}

const app    = express();
const server = createServer(app);

/* ── Socket.IO CORS ──────────────────────────────────────── */
const io = new Server(server, {
    maxHttpBufferSize: 2 * 1024 * 1024, // bounded signed overlay recovery snapshot
    pingInterval: 25_000,
    pingTimeout: 60_000,
    upgradeTimeout: 30_000,
    cors: {
        origin: (origin, callback) => {
            // Origins outside loveligo.com are allowed through CORS only so they
            // can present the test code. The Socket.IO auth middleware below is
            // the actual access gate.
            callback(null, true);
        },
        methods:          ['GET', 'POST'],
        allowedHeaders:   ['Content-Type', 'X-Loveligo-Access-Code'],
        credentials:      false,
    },
});

/* ── Express HTTP CORS middleware ────────────────────────── */
app.use((req, res, next) => {
    const origin = req.headers.origin;

    if (origin) res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Loveligo-Access-Code');
    res.setHeader('Vary', 'Origin');

    if (req.method === 'OPTIONS') {
        return res.sendStatus(204);
    }

    next();
});

const PORT = process.env.PORT || 3000;

/* ============================================================
   CONFIG
============================================================ */

const GRACE_PERIOD_MS = Math.max(30_000, Number(process.env.GRACE_PERIOD_MS) || 120_000);
const MAX_ACTIVE_TIKTOK = Math.max(1, Number(process.env.MAX_ACTIVE_TIKTOK) || 50);
const MAX_TOTAL_SOCKETS = Math.max(50, Number(process.env.MAX_TOTAL_SOCKETS) || 2_000);
const MAX_SOCKETS_PER_IP = Math.max(10, Number(process.env.MAX_SOCKETS_PER_IP) || 100);
const AUTH_FAILURE_WINDOW_MS = 10 * 60_000;
const AUTH_BLOCK_MS = 15 * 60_000;
const MAX_AUTH_FAILURES = 5;
const UPSTREAM_WINDOW_MS = 10 * 60_000;
const MAX_NEW_UPSTREAMS_PER_IP = Math.max(2, Number(process.env.MAX_NEW_UPSTREAMS_PER_IP) || 10);

/* ============================================================
   STORAGE
   liveConnections: Map<string, { adapter, roomId, graceTimer }>
============================================================ */

/** @type {Map<string, {adapter: TikTokAdapter, roomId: string, graceTimer: any}>} */
const liveConnections    = new Map();

/** يمنع إنشاء اتصالَين لنفس المستخدم في نفس الوقت */
const pendingConnections = new Set();
const socketCountsByIp = new Map();
const authFailuresByIp = new Map();
const upstreamAttemptsByIp = new Map();

/* ============================================================
   HELPERS
============================================================ */

const roomOf   = (u) => `tiktok:${u.toLowerCase()}`;
const roomSize = (r) => { const room = io.sockets.adapter.rooms.get(r); return room ? room.size : 0; };

function clientIp(socketOrRequest) {
    const headers = socketOrRequest?.handshake?.headers || socketOrRequest?.headers || {};
    const forwarded = String(headers['x-forwarded-for'] || '').split(',')[0].trim();
    return forwarded || socketOrRequest?.handshake?.address || socketOrRequest?.socket?.remoteAddress || 'unknown';
}

function pruneTimes(times, windowMs, now = Date.now()) {
    return (times || []).filter(time => now - time < windowMs);
}

function recordAuthFailure(ip) {
    const now = Date.now();
    const current = authFailuresByIp.get(ip) || { times: [], blockedUntil: 0 };
    current.times = pruneTimes(current.times, AUTH_FAILURE_WINDOW_MS, now);
    current.times.push(now);
    if (current.times.length >= MAX_AUTH_FAILURES) current.blockedUntil = now + AUTH_BLOCK_MS;
    authFailuresByIp.set(ip, current);
}

function isAuthBlocked(ip) {
    const current = authFailuresByIp.get(ip);
    if (!current) return false;
    if (current.blockedUntil > Date.now()) return true;
    if (current.blockedUntil) authFailuresByIp.delete(ip);
    return false;
}

function canCreateUpstream(ip) {
    const now = Date.now();
    const times = pruneTimes(upstreamAttemptsByIp.get(ip), UPSTREAM_WINDOW_MS, now);
    if (times.length >= MAX_NEW_UPSTREAMS_PER_IP) {
        upstreamAttemptsByIp.set(ip, times);
        return false;
    }
    times.push(now);
    upstreamAttemptsByIp.set(ip, times);
    return true;
}

const limiterCleanupTimer = setInterval(() => {
    const now = Date.now();
    for (const [ip, record] of authFailuresByIp) {
        record.times = pruneTimes(record.times, AUTH_FAILURE_WINDOW_MS, now);
        if (record.blockedUntil <= now && record.times.length === 0) authFailuresByIp.delete(ip);
    }
    for (const [ip, times] of upstreamAttemptsByIp) {
        const active = pruneTimes(times, UPSTREAM_WINDOW_MS, now);
        if (active.length) upstreamAttemptsByIp.set(ip, active);
        else upstreamAttemptsByIp.delete(ip);
    }
}, 10 * 60_000);
limiterCleanupTimer.unref();

function sanitizeTechnicalMessage(error) {
    return String(error?.message || error || 'Unknown server error')
        .replace(/([?&](?:key|token|api[_-]?key|signature)=)[^&\s]+/gi, '$1***')
        .replace(/(bearer\s+)[\w.-]+/gi, '$1***')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 320);
}

function classifyJoinError(error) {
    const technical = sanitizeTechnicalMessage(error);
    const text = technical.toLowerCase();

    if (error?.code === 'SERVER_CAPACITY' || /capacity|active stream limit/.test(text)) {
        return { code: 'SERVER_CAPACITY', message: 'الخادم يخدم العدد الأقصى من البثوث المختلفة حالياً. حاول بعد قليل.', technical };
    }
    if (error?.code === 'UPSTREAM_RATE_LIMIT') {
        return { code: 'UPSTREAM_RATE_LIMIT', message: 'تم إيقاف محاولات فتح بثوث كثيرة من هذا الاتصال مؤقتاً.', technical };
    }

    if (/offline|not live|live.*not found|room.*not found|user.*not found/.test(text)) {
        return { code: 'LIVE_OFFLINE', message: 'الحساب غير موجود أو لا يوجد بث مباشر نشط حالياً.', technical };
    }
    if (/rate.?limit|too many|\b429\b/.test(text)) {
        return { code: 'RATE_LIMITED', message: 'TikTok رفض الطلبات مؤقتاً بسبب كثرتها. حاول بعد قليل.', technical };
    }
    if (/device.?blocked|tiktok blocked|forbidden|\b401\b|\b403\b/.test(text)) {
        return { code: 'TIKTOK_BLOCKED', message: 'TikTok رفض الاتصال المباشر مؤقتاً. حاول بعد قليل.', technical };
    }
    if (/age.?restricted|18\+/.test(text)) {
        return { code: 'AGE_RESTRICTED', message: 'هذا البث مقيّد بالعمر ويحتاج جلسة TikTok مسجلة في السيرفر.', technical };
    }
    if (/timeout|timed out|econn|enotfound|fetch|network|socket|websocket/.test(text)) {
        return { code: 'UPSTREAM_NETWORK_ERROR', message: 'السيرفر يعمل، لكن تعذر عليه الوصول إلى TikTok حالياً.', technical };
    }
    return { code: 'TIKTOK_CONNECT_FAILED', message: 'وصل الطلب إلى السيرفر، لكن اتصال TikTok فشل.', technical };
}

function log(level, message) {
    const ts = new Date().toISOString();
    const fn = level === 'ERROR' ? console.error : level === 'WARN' ? console.warn : console.log;
    fn(`[${ts}] [${level}] [SERVER] ${message}`);
}

app.get('/health', (req, res) => {
    const trustedOrigin = isOriginAllowed(req.headers.origin);
    const validCode = safeCodeEquals(req.headers['x-loveligo-access-code']);
    if (!trustedOrigin && !validCode) {
        return res.status(401).json({ ok: false, code: 'AUTH_REQUIRED' });
    }
    res.json({
        ok: true,
        service: 'loveligo-tiktok-bridge',
        connector: 'piratetok-live-js-direct',
        paidSigningService: false,
        timestamp: Date.now(),
        activeConnections: liveConnections.size,
        pendingConnections: pendingConnections.size,
    });
});

io.use((socket, next) => {
    const origin = socket.handshake.headers.origin;
    const ip = clientIp(socket);
    const trustedOrigin = isOriginAllowed(origin);
    const validCode = safeCodeEquals(socket.handshake.auth?.accessCode);
    const overlayRead = overlaySessions.canRead(socket.handshake.auth);

    if (isAuthBlocked(ip)) {
        const error = new Error('تم حظر محاولات الدخول مؤقتاً بسبب تكرار الرمز الخاطئ.');
        error.data = { code: 'AUTH_RATE_LIMITED', retryAfterMs: AUTH_BLOCK_MS };
        return next(error);
    }
    if (!trustedOrigin && !validCode && !overlayRead) {
        recordAuthFailure(ip);
        const error = new Error('رمز اختبار Loveligo مطلوب لهذا المصدر.');
        error.data = { code: 'AUTH_REQUIRED' };
        return next(error);
    }
    if (io.engine.clientsCount >= MAX_TOTAL_SOCKETS) {
        const error = new Error('الخادم مشغول حالياً، حاول بعد قليل.');
        error.data = { code: 'SERVER_CAPACITY' };
        return next(error);
    }
    if ((socketCountsByIp.get(ip) || 0) >= MAX_SOCKETS_PER_IP) {
        const error = new Error('تم بلوغ حد الاتصالات لهذا العنوان.');
        error.data = { code: 'IP_CONNECTION_LIMIT' };
        return next(error);
    }

    socket.data.clientIp = ip;
    socket.data.trustedOrigin = trustedOrigin;
    socket.data.overlayReadOnly = Boolean(socket.handshake.auth?.overlayChannel);
    next();
});

/* ============================================================
   DISCONNECT TIKTOK
============================================================ */

function disconnectTikTok(username) {
    const key   = username.toLowerCase();
    const entry = liveConnections.get(key);
    if (!entry) return;

    clearGraceTimer(entry);
    entry.adapter.disconnect();
    liveConnections.delete(key);

    log('INFO', `TikTok disconnected: @${username}`);
}

function clearGraceTimer(entry) {
    if (entry.graceTimer) {
        clearTimeout(entry.graceTimer);
        entry.graceTimer = null;
    }
}

/* ============================================================
   GRACE PERIOD
============================================================ */

function scheduleDisconnect(username) {
    const key   = username.toLowerCase();
    const entry = liveConnections.get(key);
    if (!entry) return;

    clearGraceTimer(entry);

    log('INFO', `⏳ Grace started for @${username} (${GRACE_PERIOD_MS / 1000}s)`);

    entry.graceTimer = setTimeout(() => {
        if (roomSize(roomOf(username)) === 0 && !overlaySessions.hasViewers(key)) {
            log('INFO', `⌛ Grace expired @${username}`);
            disconnectTikTok(username);
        } else {
            log('INFO', `✅ Room alive @${username} — grace cancelled`);
            entry.graceTimer = null;
        }
    }, GRACE_PERIOD_MS);
}

/* ============================================================
   WAIT FOR PENDING CONNECTION
============================================================ */

function waitForPending(key, startedAt = Date.now()) {
    return new Promise((resolve, reject) => {
        const check = () => {
            if (!pendingConnections.has(key)) return resolve();
            if (Date.now() - startedAt > 45_000) return reject(new Error('Pending connection timeout'));
            setTimeout(check, 300);
        };
        check();
    });
}

/* ============================================================
   CONNECT TIKTOK
============================================================ */

async function connectTikTok(username, ip) {
    const key = username.toLowerCase();

    if (liveConnections.has(key)) {
        const entry = liveConnections.get(key);
        clearGraceTimer(entry);
        return entry.avatarUrl;
    }

    /* اتصال جارٍ — انتظر */
    if (pendingConnections.has(key)) {
        log('INFO', `Waiting for pending @${username}...`);
        await waitForPending(key);
        const completed = liveConnections.get(key);
        if (completed) {
            clearGraceTimer(completed);
            return completed.avatarUrl;
        }
        return connectTikTok(username, ip);
    }

    if (liveConnections.size + pendingConnections.size >= MAX_ACTIVE_TIKTOK) {
        const error = new Error('Active stream limit reached');
        error.code = 'SERVER_CAPACITY';
        throw error;
    }
    if (!canCreateUpstream(ip)) {
        const error = new Error('Too many new upstream connections from this IP');
        error.code = 'UPSTREAM_RATE_LIMIT';
        throw error;
    }

    pendingConnections.add(key);

    let adapter = null;
    try {
        adapter = new TikTokAdapter(username);
        const roomId  = roomOf(username);

        /* تسجيل الأحداث الأساسية المحددة مسبقاً */
        const FORWARD = [
            'tiktok_like', 'tiktok_comment', 'tiktok_gift',
            'tiktok_join', 'tiktok_follow',  'tiktok_share',
            'tiktok_error',
        ];
        for (const ev of FORWARD) {
            adapter.on(ev, (payload) => io.to(roomId).emit(ev, payload));
        }

        /* ── تسجيل باقي الأحداث المضافة ديناميكياً ── */
        const EXCLUDED_DYNAMIC = [
            EventType.connected, EventType.reconnecting, EventType.disconnected,
            EventType.like, EventType.chat, EventType.gift,
            EventType.member, EventType.follow, EventType.share,
            EventType.social, EventType.join, EventType.liveEnded
        ];
        Object.values(EventType).forEach((eventName) => {
            if (!EXCLUDED_DYNAMIC.includes(eventName)) {
                const dynamicEvName = `tiktok_${eventName}`;
                adapter.on(dynamicEvName, (payload) => io.to(roomId).emit(dynamicEvName, payload));
            }
        });

        adapter.on('tiktok_disconnected', (payload) => {
            io.to(roomId).emit('tiktok_disconnected', payload);
        });

        adapter.on('tiktok_reconnected', (payload) => {
            io.to(roomId).emit('tiktok_reconnected', payload);
            log('INFO', `Reconnected forwarded for @${username}`);
        });

        /* الاتصال */
        const state = await adapter.connect();

        let avatarUrl = null;
        try {
            avatarUrl = adapter.getStreamerProfilePicture(state) || null;
        } catch(e) {
            log('WARN', `Could not extract avatar: ${e.message}`);
        }

        liveConnections.set(key, { adapter, roomId, graceTimer: null, avatarUrl });

        log('INFO', `✅ TikTok connected @${username}`);
        return avatarUrl;

    } catch (error) {
        if (adapter) adapter.disconnect();
        throw error;

    } finally {
        pendingConnections.delete(key);
    }
}

/* ============================================================
   SOCKET.IO
============================================================ */

const overlaySessions = installOverlaySessions({io, connectTikTok, liveConnections, roomOf, roomSize, scheduleDisconnect});

io.on('connection', (socket) => {

    const ip = socket.data.clientIp || clientIp(socket);
    socketCountsByIp.set(ip, (socketCountsByIp.get(ip) || 0) + 1);
    log('INFO', `🟢 Socket: ${socket.id}`);

    /* ── UNIFIED JOIN LOGIC ───────────────────────────────── */
    const handleJoin = async (payload, acknowledge) => {
        if (socket.data.overlayReadOnly) {
            if (typeof acknowledge === 'function') acknowledge({ok:false,error:{code:'OVERLAY_READ_ONLY'}});
            return;
        }
        let acknowledged = false;
        const respond = (body) => {
            if (acknowledged || typeof acknowledge !== 'function') return;
            acknowledged = true;
            acknowledge(body);
        };
        let username = null;
        if (typeof payload === 'string') username = payload;
        else if (payload && typeof payload.username === 'string') username = payload.username;

        if (!username) {
            const error = { code: 'USERNAME_REQUIRED', message: 'اسم حساب TikTok مطلوب.' };
            socket.emit('join_error', error);
            respond({ ok: false, error });
            return;
        }

        const clean = username.replace('@', '').trim().toLowerCase();
        if (!clean) {
            const error = { code: 'INVALID_USERNAME', message: 'اسم حساب TikTok غير صالح.' };
            socket.emit('join_error', error);
            respond({ ok: false, error });
            return;
        }

        // Prevent duplicate concurrent requests from the same socket
        if (socket.currentJoinedUser === clean) {
            const active = liveConnections.get(clean);
            const existing = {
                username: clean,
                profilePictureUrl: active?.avatarUrl || '',
                alreadyJoined: true,
            };
            socket.emit('joined_game', existing);
            respond({ ok: true, data: existing });
            return;
        }
        if (socket._joiningUser === clean) {
            socket.emit('join_progress', { stage: 'tiktok_connecting', username: clean });
            respond({ ok: true, pending: true });
            return;
        }
        socket._joiningUser = clean;
        socket.emit('join_progress', { stage: 'server_received', username: clean });

        try {
            socket.emit('join_progress', { stage: 'tiktok_connecting', username: clean });
            const avatarUrl = await connectTikTok(clean, ip);

            const previousUser = socket.currentJoinedUser;
            socket.join(roomOf(clean));
            socket.currentJoinedUser = clean;
            if (previousUser && previousUser !== clean) {
                socket.leave(roomOf(previousUser));
                if (roomSize(roomOf(previousUser)) === 0) scheduleDisconnect(previousUser);
            }

            const finalPic = avatarUrl || `https://api.dicebear.com/7.x/initials/svg?seed=${clean}`;

            const joinedData = {
                username: clean,
                profilePictureUrl: finalPic,
            };
            socket.emit('joined_game', joinedData);
            respond({ ok: true, data: joinedData });

            log('INFO', `🎮 Socket ${socket.id} joined @${clean}`);

        } catch (err) {
            log('ERROR', `join_game @${clean}: ${err.message}`);
            const error = classifyJoinError(err);
            socket.emit('join_error', error);
            respond({ ok: false, error });
        } finally {
            socket._joiningUser = null;
        }
    };

    socket.on('join_game', handleJoin);
    socket.on('connect-tiktok', handleJoin);
    socket.on('join', handleJoin);
    socket.on('set_username', handleJoin);
    socket.on('room', handleJoin);

    /* ── UNIFIED LEAVE LOGIC ──────────────────────────────── */
    const handleLeave = (payload) => {
        if (socket.data.overlayReadOnly) return;
        let username = null;
        if (typeof payload === 'string') username = payload;
        else if (payload && typeof payload.username === 'string') username = payload.username;
        
        const clean = username ? username.replace('@', '').trim().toLowerCase() : socket.currentJoinedUser;
        if (!clean) return;

        socket.leave(roomOf(clean));
        if (roomSize(roomOf(clean)) === 0) scheduleDisconnect(clean);
        socket.currentJoinedUser = null;
    };

    socket.on('leave_game', handleLeave);
    socket.on('disconnect-tiktok', handleLeave);

    /* GET STATS ──────────────────────────────────────────── */
    socket.on('get_stats', ({ username } = {}) => {
        if (socket.data.overlayReadOnly) return;
        if (!username || typeof username !== 'string') return;
        const clean = username.replace('@', '').trim().toLowerCase();
        if (!clean) return;

        const entry = liveConnections.get(clean);
        socket.emit('stats', {
            username:  clean,
            stats:     entry ? entry.adapter.stats : null,
            connected: !!entry,
        });
    });

    /* GIFT CATALOG ───────────────────────────────────────── */
    socket.on('get_gift_catalog', async ({ username, force = false } = {}, acknowledge) => {
        if (socket.data.overlayReadOnly) {
            if (typeof acknowledge === 'function') acknowledge({ok:false,error:{code:'OVERLAY_READ_ONLY'}});
            return;
        }
        const clean = String(username || socket.currentJoinedUser || '')
            .replace('@', '')
            .trim()
            .toLowerCase();
        const respond = body => {
            if (typeof acknowledge === 'function') acknowledge(body);
        };

        if (!clean) {
            respond({ ok: false, error: { code: 'NOT_JOINED', message: 'اتصل ببث TikTok أولاً.' } });
            return;
        }

        const entry = liveConnections.get(clean);
        if (!entry) {
            respond({ ok: false, error: { code: 'STREAM_NOT_CONNECTED', message: 'اتصال البث غير متاح حالياً.' } });
            return;
        }

        try {
            const gifts = await entry.adapter.getAvailableGifts({ force: Boolean(force) });
            const payload = { ok: true, username: clean, gifts, fetchedAt: Date.now() };
            socket.emit('gift_catalog', payload);
            respond(payload);
        } catch (error) {
            const classified = classifyJoinError(error);
            log('ERROR', `gift_catalog @${clean}: ${classified.technical}`);
            respond({ ok: false, error: classified });
        }
    });

    /* SOCKET DISCONNECT ──────────────────────────────────── */
    socket.on('disconnect', () => {
        log('INFO', `🔴 Socket: ${socket.id}`);

        const remaining = Math.max(0, (socketCountsByIp.get(ip) || 1) - 1);
        if (remaining) socketCountsByIp.set(ip, remaining);
        else socketCountsByIp.delete(ip);

        const joinedUser = socket.currentJoinedUser;
        if (!joinedUser) return;

        setImmediate(() => {
            if (roomSize(roomOf(joinedUser)) === 0) scheduleDisconnect(joinedUser);
        });
    });
});

/* ============================================================
   START
============================================================ */

server.listen(PORT, () => {
    log('INFO', `🚀 Bridge Server → http://localhost:${PORT}`);
    log('INFO', `⏳ Grace Period      : ${GRACE_PERIOD_MS / 1000}s`);
    log('INFO', `🔄 Auto reconnect     : unlimited background retry`);
    log('INFO', `📦 Library           : piratetok-live-js (direct WSS)`);
    log('INFO', `🔧 Connection Class  : TikTokLiveClient`);
    log('INFO', `💸 Signing service    : disabled (no Euler/API key)`);
    log('INFO', `🔒 Direct origin      : https://loveligo.com and subdomains`);
    log('INFO', `🧪 Other origins      : test access code required`);
    log('INFO', `📊 Capacity           : ${MAX_ACTIVE_TIKTOK} TikTok streams / ${MAX_TOTAL_SOCKETS} viewers`);
});
