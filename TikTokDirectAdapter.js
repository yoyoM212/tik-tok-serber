/**
 * Free TikTok LIVE adapter for Loveligo.
 *
 * It connects directly through piratetok-live-js. There is deliberately no
 * EulerStream URL, signing API key, or automatic paid-service fallback here.
 * Its public `tiktok_*` events match the legacy adapter so games stay unchanged.
 */

import { EventEmitter } from 'node:events';
import { TikTokLiveClient, EventType } from 'piratetok-live-js';

const RECONNECT_DELAY_MS = 5_000;
const RECONNECT_DELAY_MAX_MS = 60_000;
const CONNECT_READY_DELAY_MS = 900;

/*
 * piratetok-live-js@0.1.5 still asks the TikTok homepage for `ttwid`.
 * TikTok now issues that anonymous cookie from /ttwid/register/ instead.
 * Redirect only that exact internal request; all other global fetch calls are
 * passed through untouched. This endpoint is TikTok itself, not a sign service.
 */
const TTWID_FETCH_PATCH = Symbol.for('loveligo.tiktok.ttwid-fetch-patch');
if (!globalThis[TTWID_FETCH_PATCH]) {
    const nativeFetch = globalThis.fetch.bind(globalThis);
    globalThis.fetch = (input, init = {}) => {
        const url = typeof input === 'string' ? input : input?.url;
        const method = String(init?.method || input?.method || 'GET').toUpperCase();
        const isLegacyTtwidProbe = method === 'GET'
            && init?.redirect === 'manual'
            && /^https:\/\/www\.tiktok\.com\/?$/.test(String(url || ''));

        if (!isLegacyTtwidProbe) return nativeFetch(input, init);

        const headers = new Headers(init.headers || input?.headers || {});
        headers.set('Content-Type', 'application/x-www-form-urlencoded');
        return nativeFetch('https://www.tiktok.com/ttwid/register/', {
            ...init,
            method: 'POST',
            headers,
            body: JSON.stringify({
                aid: 1988,
                service: 'www.tiktok.com',
                union: false,
                unionHost: '',
                needFid: false,
                fid: '',
            }),
        });
    };
    globalThis[TTWID_FETCH_PATCH] = true;
}

function log(level, username, message, extra) {
    const output = `[${new Date().toISOString()}] [${level}] [@${username}] ${message}`;
    if (level === 'ERROR') console.error(output, extra ?? '');
    else if (level === 'WARN') console.warn(output, extra ?? '');
    else console.log(output, extra ?? '');
}

function finiteNumber(...values) {
    for (const value of values) {
        const number = Number(value);
        if (Number.isFinite(number)) return number;
    }
    return 0;
}

function normalizeGiftName(value) {
    return String(value ?? '')
        .normalize('NFKC')
        .trim()
        .toLocaleLowerCase('en-US')
        .replace(/[\s_-]+/g, ' ');
}

function firstImageUrl(value) {
    if (!value) return '';
    if (typeof value === 'string') return /^https?:\/\//i.test(value) ? value : '';
    if (Array.isArray(value)) {
        for (const item of value) {
            const result = firstImageUrl(item);
            if (result) return result;
        }
        return '';
    }
    if (typeof value === 'object') {
        return firstImageUrl(value.urlList || value.url_list || value.urls || value.url || value.uri);
    }
    return '';
}

function unwrapEvent(value) {
    // v0.1.x emits decoded data directly; tolerate a future event wrapper too.
    if (value && typeof value === 'object' && value.type && value.data) return value.data;
    return value || {};
}

function extractUser(input) {
    const data = unwrapEvent(input);
    const user = data?.user || data?.userDetails || data || {};
    return {
        uniqueId: String(user.uniqueId || user.displayId || data.uniqueId || data.displayId || ''),
        nickname: String(user.nickname || data.nickname || ''),
        profilePictureUrl: firstImageUrl(
            user.avatarLarge
            || user.avatarMedium
            || user.avatarThumb
            || user.profilePictureUrl
            || data.profilePictureUrl
            || data.avatarUrl
        ),
    };
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchStreamerProfile(username, timeoutMs = 10_000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    const language = String(process.env.TIKTOK_LANGUAGE || 'en').trim() || 'en';
    const region = String(process.env.TIKTOK_REGION || 'US').trim() || 'US';
    const params = new URLSearchParams({
        aid: '1988',
        app_name: 'tiktok_web',
        device_platform: 'web_pc',
        app_language: language,
        browser_language: `${language}-${region}`,
        region,
        user_is_login: 'false',
        sourceType: '54',
        staleTime: '600000',
        uniqueId: username,
    });
    try {
        const response = await fetch(`https://www.tiktok.com/api-live/user/room?${params}`, {
            headers: { 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36' },
            signal: controller.signal,
        });
        if (!response.ok) throw new Error(`profile HTTP ${response.status}`);
        const body = await response.json();
        const user = body?.data?.user;
        if (!user) throw new Error('profile user data missing');
        return {
            userId: String(user.id || ''),
            uniqueId: String(user.uniqueId || username),
            nickname: String(user.nickname || ''),
            avatarThumb: String(user.avatarThumb || ''),
            avatarMedium: String(user.avatarMedium || ''),
            avatarLarge: String(user.avatarLarger || user.avatarLarge || ''),
            verified: Boolean(user.verified),
            roomId: String(user.roomId || ''),
        };
    } finally {
        clearTimeout(timer);
    }
}

export default class TikTokDirectAdapter extends EventEmitter {
    constructor(username, options = {}) {
        super();
        this.username = String(username || '').replace(/^@/, '').trim().toLowerCase();
        this._options = options;
        this._conn = null;
        this._runPromise = null;
        this._connectPromise = null;
        this._reconnectTimer = null;
        this._reconnectTries = 0;
        this._destroyed = false;
        this._connected = false;
        this._recovering = false;
        this._roomId = '';
        this._streamerProfile = null;
        this._profilePromise = null;
        this._giftCatalog = new Map();
        this._userLikes = Object.create(null);
        this.stats = { likes: 0, comments: 0, gifts: 0 };
    }

    async connect() {
        if (this._destroyed) throw new Error(`Adapter @${this.username} is destroyed`);
        if (this._connected && this._roomId) return this._state();
        if (this._connectPromise) return this._connectPromise;

        this._connectPromise = this._startClient();
        try {
            return await this._connectPromise;
        } finally {
            this._connectPromise = null;
        }
    }

    disconnect() {
        this._destroyed = true;
        this._connected = false;
        this._recovering = false;
        this._clearReconnectTimer();

        const client = this._conn;
        this._conn = null;
        if (client) {
            client.removeAllListeners();
            if (typeof client.disconnect === 'function') client.disconnect();
        }
        this._runPromise = null;
        this._userLikes = Object.create(null);
        log('INFO', this.username, 'Direct TikTok connection closed 🔌');
    }

    isConnected() {
        return this._connected && !this._destroyed;
    }

    getStreamerProfilePicture() {
        return this._streamerProfile?.avatarLarge
            || this._streamerProfile?.avatarMedium
            || this._streamerProfile?.avatarThumb
            || '';
    }

    async getAvailableGifts() {
        // The direct stream includes metadata for every received gift. The
        // signed TikTok gift-list route is not called, so this list is learned
        // from gifts observed during the current broadcast.
        return Array.from(this._giftCatalog.values()).sort((a, b) =>
            a.diamondCount - b.diamondCount || a.name.localeCompare(b.name)
        );
    }

    _state() {
        return {
            roomId: this._roomId,
            roomInfo: this._streamerProfile ? { owner: this._streamerProfile } : null,
            isConnected: this._connected,
        };
    }

    _makeClient() {
        const client = new TikTokLiveClient(this.username)
            .timeout(Math.max(5_000, finiteNumber(this._options.timeoutMs, 15_000)))
            .maxRetries(Math.max(0, finiteNumber(this._options.maxRetries, 5)))
            .staleTimeout(Math.max(30_000, finiteNumber(this._options.staleTimeoutMs, 90_000)));

        const region = String(this._options.region || process.env.TIKTOK_REGION || '').trim();
        const language = String(this._options.language || process.env.TIKTOK_LANGUAGE || '').trim();
        const cdn = String(this._options.cdn || process.env.TIKTOK_CDN || '').trim();
        const cookies = String(this._options.cookies || process.env.TIKTOK_SESSION_COOKIES || '').trim();
        if (region) client.region(region);
        if (language) client.language(language);
        if (cdn) client.cdn(cdn);
        if (cookies) client.cookies(cookies);
        return client;
    }

    async _startClient() {
        const client = this._makeClient();
        this._conn = client;
        this._attachEvents(client);
        log('INFO', this.username, 'Connecting directly to TikTok LIVE (no sign server)...');

        let readyResolve;
        let readyReject;
        const ready = new Promise((resolve, reject) => {
            readyResolve = resolve;
            readyReject = reject;
        });

        client.once(EventType.connected, async ({ roomId } = {}) => {
            if (client !== this._conn || this._destroyed) return;
            this._roomId = String(roomId || '');
            this._connected = true;
            this._reconnectTries = 0;
            const profileTask = this._loadStreamerProfile();
            await Promise.all([
                delay(CONNECT_READY_DELAY_MS),
                Promise.race([profileTask, delay(3_000)]),
            ]);
            if (client !== this._conn || this._destroyed) return;
            log('INFO', this.username, `Direct connection started ✅ roomId=${this._roomId || 'N/A'}`);
            readyResolve(this._state());
        });

        this._runPromise = client.connect();
        this._runPromise
            .then(() => {
                if (client !== this._conn || this._destroyed) return;
                if (!this._roomId) readyReject(new Error('TikTok direct connection ended before it became ready'));
            })
            .catch((error) => {
                if (client !== this._conn || this._destroyed) return;
                readyReject(error);
                this._handleTerminalDisconnect(client, error);
            });
        return ready;
    }

    _attachEvents(client) {
        const u = this.username;
        const active = () => client === this._conn && !this._destroyed;

        client.on(EventType.chat, (input) => {
            if (!active()) return;
            this._markActivity();
            const data = unwrapEvent(input);
            const user = extractUser(data);
            this.stats.comments++;
            this.emit('tiktok_comment', {
                streamer: u,
                commenter: user.uniqueId,
                nickname: user.nickname,
                profilePictureUrl: user.profilePictureUrl,
                comment: String(data.content ?? data.comment ?? ''),
                timestamp: Date.now(),
            });
        });

        client.on(EventType.like, (input) => {
            if (!active()) return;
            this._markActivity();
            const data = unwrapEvent(input);
            const user = extractUser(data);
            const likes = Math.max(1, finiteNumber(data.count, data.likeCount, 1));
            const uid = user.uniqueId;
            this._userLikes[uid] = (this._userLikes[uid] || 0) + likes;
            this.stats.likes += likes;
            this.emit('tiktok_like', {
                streamer: u,
                liker: uid,
                nickname: user.nickname,
                profilePictureUrl: user.profilePictureUrl,
                likeCount: likes,
                userTotalLikes: this._userLikes[uid],
                streamTotalLikes: Math.max(this.stats.likes, finiteNumber(data.total, data.totalLikes)),
                timestamp: Date.now(),
            });
        });

        client.on(EventType.gift, (input) => {
            if (!active()) return;
            this._markActivity();
            const data = unwrapEvent(input);
            const user = extractUser(data);
            const gift = data.gift || {};
            const giftId = finiteNumber(data.giftId, gift.id);
            const giftName = String(gift.name || data.giftName || `Gift#${giftId}`);
            const diamondCount = Math.max(0, finiteNumber(gift.diamondCount, data.diamondCount));
            const repeatCount = Math.max(1, finiteNumber(data.repeatCount, data.comboCount, 1));
            const giftType = finiteNumber(gift.type, data.giftType);
            const isStreakable = Boolean(gift.combo) || giftType === 1;
            const isStreakEnd = data.repeatEnd !== undefined
                ? Boolean(finiteNumber(data.repeatEnd))
                : !isStreakable;
            const timestamp = Date.now();
            const eventId = String(data.common?.msgId || [u, user.uniqueId, giftId, repeatCount, Math.floor(timestamp / 250)].join(':'));
            const image = firstImageUrl(gift.image);

            this._rememberGift({ id: giftId, name: giftName, diamondCount, type: giftType, image });
            this.stats.gifts++;
            this.emit('tiktok_gift', {
                schemaVersion: 2,
                streamer: u,
                gifter: user.uniqueId,
                nickname: user.nickname,
                profilePictureUrl: user.profilePictureUrl,
                giftId,
                giftName,
                giftNameNormalized: normalizeGiftName(giftName),
                giftPictureUrl: image,
                giftImageUrl: image,
                repeatCount,
                diamondCount,
                totalDiamonds: diamondCount * repeatCount,
                giftType,
                isStreakable,
                isStreakEnd,
                repeatEnd: isStreakEnd,
                eventId,
                timestamp,
            });
        });

        client.on(EventType.join, input => {
            if (!active()) return;
            this._markActivity();
            const user = extractUser(input);
            this.emit('tiktok_join', {
                streamer: u,
                member: user.uniqueId,
                nickname: user.nickname,
                profilePictureUrl: user.profilePictureUrl,
                timestamp: Date.now(),
            });
        });

        client.on(EventType.follow, input => {
            if (!active()) return;
            this._markActivity();
            const user = extractUser(input);
            this.emit('tiktok_follow', {
                streamer: u,
                follower: user.uniqueId,
                nickname: user.nickname,
                profilePictureUrl: user.profilePictureUrl,
                timestamp: Date.now(),
            });
        });

        client.on(EventType.share, input => {
            if (!active()) return;
            this._markActivity();
            const user = extractUser(input);
            this.emit('tiktok_share', {
                streamer: u,
                username: user.uniqueId,
                nickname: user.nickname,
                profilePictureUrl: user.profilePictureUrl,
                timestamp: Date.now(),
            });
        });

        client.on(EventType.reconnecting, (details = {}) => {
            if (!active()) return;
            if (!this._recovering) {
                this._recovering = true;
                this.emit('tiktok_disconnected', { username: u, temporary: true });
            }
            log('WARN', u, `Direct WSS reconnecting (attempt ${details?.attempt ?? '?'})`);
        });

        client.on(EventType.liveEnded, () => {
            if (!active()) return;
            this._connected = false;
            this.emit('tiktok_disconnected', { username: u, streamEnded: true });
            log('INFO', u, 'TikTok LIVE ended');
        });

        client.on(EventType.disconnected, () => {
            if (active()) this._handleTerminalDisconnect(client);
        });

        client.on('error', error => {
            if (!active()) return;
            const message = error?.message || String(error);
            log('ERROR', u, 'Direct TikTok error:', message);
            this.emit('tiktok_error', { message });
        });

        const coreEvents = new Set([
            EventType.connected, EventType.reconnecting, EventType.disconnected,
            EventType.chat, EventType.like, EventType.gift,
            EventType.member, EventType.social, EventType.join,
            EventType.follow, EventType.share, EventType.liveEnded,
        ]);
        for (const eventName of Object.values(EventType)) {
            if (coreEvents.has(eventName)) continue;
            client.on(eventName, input => {
                if (!active()) return;
                this._markActivity();
                const data = unwrapEvent(input);
                const user = extractUser(data);
                this.emit(`tiktok_${eventName}`, {
                    streamer: u,
                    userId: user.uniqueId,
                    nickname: user.nickname,
                    profilePictureUrl: user.profilePictureUrl,
                    timestamp: Date.now(),
                    rawData: data,
                });
            });
        }
        log('INFO', u, 'Events attached ✅ (piratetok-live-js direct WSS)');
    }

    _markActivity() {
        this._connected = true;
        if (!this._recovering) return;
        this._recovering = false;
        this._reconnectTries = 0;
        this.emit('tiktok_reconnected', { username: this.username });
        log('INFO', this.username, 'Direct TikTok events resumed ✅');
    }

    _rememberGift(gift) {
        const key = gift.id ? `id:${gift.id}` : `name:${normalizeGiftName(gift.name)}`;
        const previous = this._giftCatalog.get(key) || {};
        this._giftCatalog.set(key, { ...previous, ...gift, image: gift.image || previous.image || '' });
    }

    _loadStreamerProfile() {
        if (this._streamerProfile) return Promise.resolve(this._streamerProfile);
        if (this._profilePromise) return this._profilePromise;
        if (this._destroyed) return Promise.resolve(null);
        this._profilePromise = fetchStreamerProfile(this.username)
            .then(profile => {
                if (!this._destroyed) this._streamerProfile = profile;
                return profile;
            })
            .catch(error => {
                log('WARN', this.username, `Streamer avatar unavailable: ${error?.message || error}`);
                return null;
            })
            .finally(() => {
                this._profilePromise = null;
            });
        return this._profilePromise;
    }

    _handleTerminalDisconnect(client, error = null) {
        if (client !== this._conn || this._destroyed) return;
        this._connected = false;
        this._recovering = false;
        client.removeAllListeners();
        this._conn = null;
        this._runPromise = null;
        this.emit('tiktok_disconnected', { username: this.username });
        if (error) log('ERROR', this.username, 'Direct connection stopped:', error?.message || error);
        this._scheduleReconnect();
    }

    _scheduleReconnect() {
        if (this._destroyed || this._reconnectTimer) return;
        this._reconnectTries++;
        const baseDelay = Math.min(
            RECONNECT_DELAY_MS * (2 ** Math.min(this._reconnectTries - 1, 4)),
            RECONNECT_DELAY_MAX_MS
        );
        const delayMs = Math.round(baseDelay * (0.85 + Math.random() * 0.3));
        log('INFO', this.username, `Restarting direct connection in ${delayMs}ms`);
        this._reconnectTimer = setTimeout(async () => {
            this._reconnectTimer = null;
            if (this._destroyed) return;
            try {
                await this.connect();
                this.emit('tiktok_reconnected', { username: this.username });
            } catch (error) {
                log('ERROR', this.username, 'Direct reconnect failed:', error?.message || error);
                this._scheduleReconnect();
            }
        }, delayMs);
    }

    _clearReconnectTimer() {
        if (!this._reconnectTimer) return;
        clearTimeout(this._reconnectTimer);
        this._reconnectTimer = null;
    }
}
