/* ============================================================
   REALTIME ROOM SYNC — Mâm Đền
   Đồng bộ bảng điểm realtime nhiều thiết bị qua Firebase Realtime Database.
   - Chủ phòng (host): điều khiển điểm → đẩy state lên Firebase sau mỗi thay đổi.
   - Người xem (viewer): nhập mã / quét QR → nhận state realtime, chỉ XEM.
   - Tự sinh Room ID dạng ROOM-XXXX, có QR + link chia sẻ.
   - F5 / mở lại trình duyệt → tự kết nối lại phòng cũ (đọc từ Firebase).

   Nếu chưa cấu hình Firebase (firebase-config.js còn "YOUR_..."), module tự
   chuyển sang CHẾ ĐỘ DEMO NỘI BỘ (BroadcastChannel) để thử giữa các tab cùng
   trình duyệt. Cấu hình Firebase để đồng bộ thật giữa các thiết bị.
   ============================================================ */
(function () {
  'use strict';

  var ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // bỏ ký tự dễ nhầm (0/O/1/I)
  var LS_ROOM  = 'rt_room';          // {roomId, role}
  var LS_DATA  = 'rt_room_data_';    // (demo) + roomId -> payload gần nhất

  /* ── tiện ích ───────────────────────────────────────────── */
  function genRoomId() {
    var s = '';
    for (var i = 0; i < 4; i++) s += ALPHABET[Math.floor(Math.random() * ALPHABET.length)];
    return 'ROOM-' + s;
  }
  function normalizeCode(input) {
    if (!input) return null;
    var c = String(input).trim().toUpperCase().replace(/\s+/g, '');
    var m = c.match(/([A-Z0-9]{4})$/); // lấy 4 ký tự cuối (chấp nhận '8X3K' hoặc 'ROOM-8X3K')
    return m ? 'ROOM-' + m[1] : null;
  }
  function roomUrl(id) {
    return location.origin + location.pathname + '?room=' + id;
  }
  function readPersist() {
    try { return JSON.parse(localStorage.getItem(LS_ROOM) || 'null'); } catch (_) { return null; }
  }
  function persistRoom(id, role) {
    try { localStorage.setItem(LS_ROOM, JSON.stringify({ roomId: id, role: role })); } catch (_) {}
  }
  function clearPersist() {
    try { localStorage.removeItem(LS_ROOM); } catch (_) {}
  }

  /* ── đóng gói payload lưu lên Firebase ──────────────────── */
  function wrapPayload(roomId, data, isCreate) {
    var g = data && data.game;
    var p = (g && g.players) || [];
    var payload = {
      roomId: roomId,
      updatedAt: Date.now(),
      matchStatus: g ? 'live' : 'waiting',
      state: data || null,
      // các trường tiện tra cứu theo yêu cầu
      player1Name:  p[0] ? p[0].name  : null,
      player1Score: p[0] ? p[0].score : null,
      player2Name:  p[1] ? p[1].name  : null,
      player2Score: p[1] ? p[1].score : null
    };
    if (isCreate) payload.createdAt = Date.now();
    return payload;
  }

  /* ── Provider: Firebase RDB ─────────────────────────────── */
  function firebaseProvider(db) {
    return {
      name: 'firebase',
      set: function (roomId, payload) { return db.ref('rooms/' + roomId).set(payload); },
      update: function (roomId, payload) { return db.ref('rooms/' + roomId).update(payload); },
      subscribe: function (roomId, cb) {
        var ref = db.ref('rooms/' + roomId);
        var h = ref.on('value', function (snap) { cb(snap.val()); });
        return function () { ref.off('value', h); };
      }
    };
  }

  /* ── Provider: BroadcastChannel (demo cùng trình duyệt) ──── */
  function broadcastProvider() {
    var chans = {};
    function chan(roomId) { return chans[roomId] || (chans[roomId] = new BroadcastChannel('rt-' + roomId)); }
    function read(roomId) { try { return JSON.parse(localStorage.getItem(LS_DATA + roomId) || 'null'); } catch (_) { return null; } }
    function write(roomId, d) { try { localStorage.setItem(LS_DATA + roomId, JSON.stringify(d)); } catch (_) {} }
    return {
      name: 'broadcast',
      set: function (roomId, payload) { write(roomId, payload); chan(roomId).postMessage(payload); return Promise.resolve(); },
      update: function (roomId, payload) {
        var merged = Object.assign({}, read(roomId) || {}, payload);
        write(roomId, merged); chan(roomId).postMessage(merged); return Promise.resolve();
      },
      subscribe: function (roomId, cb) {
        var c = chan(roomId);
        var onMsg = function (e) { cb(e.data); };
        var onStorage = function (e) { if (e.key === LS_DATA + roomId && e.newValue) { try { cb(JSON.parse(e.newValue)); } catch (_) {} } };
        c.addEventListener('message', onMsg);
        window.addEventListener('storage', onStorage);
        Promise.resolve().then(function () { var d = read(roomId); if (d) cb(d); }); // giá trị ban đầu
        return function () { c.removeEventListener('message', onMsg); window.removeEventListener('storage', onStorage); };
      }
    };
  }

  var provider = null;
  function initProvider() {
    var cfg = window.firebaseConfig;
    var configured = cfg && typeof cfg.apiKey === 'string' && cfg.apiKey.indexOf('YOUR_') !== 0 && window.firebase;
    if (configured) {
      try {
        if (!firebase.apps.length) firebase.initializeApp(cfg);
        provider = firebaseProvider(firebase.database());
        return;
      } catch (e) { console.warn('[realtime] Firebase init lỗi → chuyển demo nội bộ:', e && e.message); }
    }
    provider = broadcastProvider();
  }
  function isFirebase() { return provider && provider.name === 'firebase'; }

  /* ── RoomSync (API công khai) ───────────────────────────── */
  var RoomSync = window.RoomSync = {
    role: null,      // 'host' | 'viewer' | null
    roomId: null,
    _unsub: null,
    _gotData: false,

    // được saveState() gọi sau mỗi thay đổi (chỉ host mới đẩy lên)
    _onLocalSave: function (data) {
      if (this.role === 'host' && this.roomId && provider) {
        provider.update(this.roomId, wrapPayload(this.roomId, data, false));
      }
    },

    // HOST: tạo / mở lại phòng chia sẻ
    createRoom: function () {
      if (this.role === 'viewer') this.leave(true);
      if (this.role === 'host' && this.roomId) { openShareModal(this.roomId); return; }
      var id = genRoomId();
      this.role = 'host';
      this.roomId = id;
      persistRoom(id, 'host');
      var data = (typeof window.buildStateData === 'function') ? window.buildStateData() : null;
      if (provider) provider.set(id, wrapPayload(id, data, true));
      showHostBadge(id);
      openShareModal(id);
    },

    // HOST khi tải lại trang: gắn lại phòng cũ và đẩy state hiện tại
    _rehost: function (id) {
      this.role = 'host';
      this.roomId = id;
      showHostBadge(id);
      var data = (typeof window.buildStateData === 'function') ? window.buildStateData() : null;
      if (provider && data) provider.update(id, wrapPayload(id, data, false));
    },

    // VIEWER: tham gia xem 1 phòng
    joinRoom: function (input) {
      var code = normalizeCode(input);
      if (!code) { toast('Mã phòng không hợp lệ'); return; }
      this.leave(true);
      this.role = 'viewer';
      this.roomId = code;
      this._gotData = false;
      persistRoom(code, 'viewer');
      document.body.classList.add('ro-viewer');
      closeModal('rtJoinModal');
      showViewerBadge(code);
      showWaiting('Đang kết nối…');
      var self = this;
      this._unsub = provider.subscribe(code, function (payload) {
        if (!payload) {
          if (!self._gotData) showWaiting('Chưa có dữ liệu phòng ' + code + '.\nKiểm tra lại mã hoặc chờ chủ phòng bắt đầu.');
          return;
        }
        self._gotData = true;
        if (payload.matchStatus === 'ended') { showWaiting('Chủ phòng đã kết thúc chia sẻ.'); return; }
        if (payload.state && payload.state.game) {
          hideWaiting();
          if (typeof window.applyRemoteState === 'function') window.applyRemoteState(payload.state);
        } else {
          showWaiting('Đang chờ trận bắt đầu…');
        }
      });
    },

    // rời phòng (viewer) hoặc dừng chia sẻ (host)
    leave: function (silent) {
      if (this._unsub) { try { this._unsub(); } catch (_) {} this._unsub = null; }
      if (this.role === 'host' && this.roomId && provider) {
        try { provider.update(this.roomId, { matchStatus: 'ended', updatedAt: Date.now() }); } catch (_) {}
      }
      this.role = null; this.roomId = null; this._gotData = false;
      clearPersist();
      document.body.classList.remove('ro-viewer');
      hideViewerBadge(); hideHostBadge(); hideWaiting();
      if (!silent) toast('Đã rời phòng');
    },

    leaveViewer: function () {
      this.leave(false);
      // viewer thoát → về màn hình chính
      if (typeof window.doExitGame === 'function') {
        // doExitGame cũng gọi leave (đã no-op vì role null) rồi về home
      }
      ['screenGame', 'screenMode', 'screenPlayers'].forEach(function (id) {
        document.getElementById(id) && document.getElementById(id).classList.add('hidden');
      });
      var home = document.getElementById('screenHome');
      if (home) home.classList.remove('hidden');
      // xoá ?room= khỏi URL cho sạch
      try { history.replaceState({}, '', location.origin + location.pathname); } catch (_) {}
    }
  };

  /* ── UI: tiêm CSS-class có sẵn trong styles.css ──────────── */
  function el(tag, cls, html) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html != null) e.innerHTML = html;
    return e;
  }

  // Toast nhỏ
  var toastTimer = null;
  function toast(msg) {
    var t = document.getElementById('rtToast');
    if (!t) { t = el('div', 'rt-toast'); t.id = 'rtToast'; document.body.appendChild(t); }
    t.textContent = msg;
    t.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { t.classList.remove('show'); }, 2200);
  }

  // Overlay dùng chung
  function ensureOverlay(id) {
    var o = document.getElementById(id);
    if (o) return o;
    o = el('div', 'rt-overlay hidden'); o.id = id;
    o.addEventListener('click', function (e) { if (e.target === o) o.classList.add('hidden'); });
    document.body.appendChild(o);
    return o;
  }
  function closeModal(id) { var o = document.getElementById(id); if (o) o.classList.add('hidden'); }

  /* Share modal (host) */
  function openShareModal(id) {
    var o = ensureOverlay('rtShareModal');
    var url = roomUrl(id);
    var demo = !isFirebase();
    o.innerHTML =
      '<div class="rt-box">' +
        '<button class="rt-x" data-close>✕</button>' +
        '<div class="rt-title">📡 Chia sẻ trận đấu</div>' +
        '<div class="rt-sub">Người khác nhập mã hoặc quét QR để XEM trực tiếp</div>' +
        '<div class="rt-code" id="rtCodeText">' + id + '</div>' +
        '<div class="rt-qr" id="rtQr"></div>' +
        '<div class="rt-url" id="rtUrl">' + url + '</div>' +
        '<div class="rt-btn-row">' +
          '<button class="rt-btn rt-btn-gold" data-copy>📋 Copy link</button>' +
          '<button class="rt-btn rt-btn-ghost" data-stop>Dừng chia sẻ</button>' +
        '</div>' +
        (demo ? '<div class="rt-note">⚠ Chế độ demo nội bộ (cùng trình duyệt). Cấu hình Firebase để xem trên thiết bị khác.</div>'
              : '') +
      '</div>';
    o.classList.remove('hidden');
    renderQR(document.getElementById('rtQr'), url);
    o.querySelector('[data-close]').onclick = function () { o.classList.add('hidden'); };
    o.querySelector('[data-copy]').onclick = function () { copyText(url); };
    o.querySelector('[data-stop]').onclick = function () {
      RoomSync.leave(false); o.classList.add('hidden');
    };
  }

  /* Join modal (viewer) */
  function openJoinModal() {
    var o = ensureOverlay('rtJoinModal');
    var demo = !isFirebase();
    o.innerHTML =
      '<div class="rt-box">' +
        '<button class="rt-x" data-close>✕</button>' +
        '<div class="rt-title">👁 Xem trận đấu</div>' +
        '<div class="rt-sub">Nhập mã phòng được chia sẻ</div>' +
        '<input class="rt-input" id="rtJoinInput" placeholder="VD: ROOM-8X3K" maxlength="12" autocomplete="off" />' +
        '<div class="rt-btn-row">' +
          '<button class="rt-btn rt-btn-gold" data-join>Tham gia xem</button>' +
        '</div>' +
        (demo ? '<div class="rt-note">⚠ Chưa cấu hình Firebase — chỉ xem được giữa các tab cùng trình duyệt.</div>' : '') +
      '</div>';
    o.classList.remove('hidden');
    var input = document.getElementById('rtJoinInput');
    setTimeout(function () { input && input.focus(); }, 50);
    o.querySelector('[data-close]').onclick = function () { o.classList.add('hidden'); };
    o.querySelector('[data-join]').onclick = function () { RoomSync.joinRoom(input.value); };
    input.addEventListener('keydown', function (e) { if (e.key === 'Enter') RoomSync.joinRoom(input.value); });
  }

  /* Badge chủ phòng */
  function showHostBadge(id) {
    var b = document.getElementById('rtHostBadge');
    if (!b) {
      b = el('button', 'rt-host-badge'); b.id = 'rtHostBadge';
      b.onclick = function () { openShareModal(RoomSync.roomId); };
      document.body.appendChild(b);
    }
    b.innerHTML = '<span class="rt-dot"></span>📡 ' + id;
    b.classList.add('show');
  }
  function hideHostBadge() { var b = document.getElementById('rtHostBadge'); if (b) b.classList.remove('show'); }

  /* Badge người xem */
  function showViewerBadge(id) {
    var b = document.getElementById('rtViewerBadge');
    if (!b) { b = el('div', 'rt-viewer-badge'); b.id = 'rtViewerBadge'; document.body.appendChild(b); }
    b.innerHTML = '<span class="rt-dot"></span>👁 ĐANG XEM · ' + id +
      '<button class="rt-leave" title="Rời phòng">✕</button>';
    b.classList.add('show');
    b.querySelector('.rt-leave').onclick = function () { RoomSync.leaveViewer(); };
  }
  function hideViewerBadge() { var b = document.getElementById('rtViewerBadge'); if (b) b.classList.remove('show'); }

  /* Overlay "đang chờ" cho viewer */
  function showWaiting(msg) {
    var o = document.getElementById('rtWaiting');
    if (!o) {
      o = el('div', 'rt-waiting'); o.id = 'rtWaiting';
      document.body.appendChild(o);
    }
    o.innerHTML = '<div class="rt-waiting-box"><div class="rt-spinner"></div><div class="rt-waiting-msg"></div>' +
      '<button class="rt-btn rt-btn-ghost" id="rtWaitLeave">Rời phòng</button></div>';
    o.querySelector('.rt-waiting-msg').textContent = msg;
    o.querySelector('#rtWaitLeave').onclick = function () { RoomSync.leaveViewer(); };
    o.classList.add('show');
  }
  function hideWaiting() { var o = document.getElementById('rtWaiting'); if (o) o.classList.remove('show'); }

  /* QR + copy */
  function renderQR(box, url) {
    if (!box) return;
    box.innerHTML = '';
    if (typeof window.qrcode !== 'function') { box.textContent = url; return; }
    try {
      var qr = window.qrcode(0, 'M');
      qr.addData(url);
      qr.make();
      box.innerHTML = qr.createImgTag(5, 8);
      var img = box.querySelector('img');
      if (img) { img.style.width = '180px'; img.style.height = '180px'; img.style.imageRendering = 'pixelated'; }
    } catch (e) { box.textContent = url; }
  }
  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () { toast('Đã copy link'); },
        function () { toast('Không copy được'); });
    } else { toast('Trình duyệt không hỗ trợ copy'); }
  }

  /* ── gắn nút vào giao diện sẵn có ────────────────────────── */
  function injectEntryPoints() {
    // Nút "Xem trận đấu" ở màn hình chính
    var home = document.getElementById('screenHome');
    if (home && !document.getElementById('rtJoinBtn')) {
      var b = el('button', 'btn btn-secondary rt-join-btn');
      b.id = 'rtJoinBtn'; b.type = 'button'; b.textContent = '📡 Xem trận đấu';
      b.onclick = openJoinModal;
      home.appendChild(b);
    }
    // Mục "Chia sẻ trận" trong dropdown menu khi đang chơi
    var dd = document.getElementById('gameDropdown');
    if (dd && !document.getElementById('rtShareItem')) {
      var it = el('button', 'dropdown-item rt-share-item', '<span class="di-icon">📡</span> Chia sẻ trận');
      it.id = 'rtShareItem';
      it.onclick = function () { dd.classList.add('hidden'); RoomSync.createRoom(); };
      dd.insertBefore(it, dd.firstChild);
    }
  }

  /* ── khởi động ───────────────────────────────────────────── */
  function autostart() {
    initProvider();
    injectEntryPoints();

    var urlRoom = null;
    try { urlRoom = new URLSearchParams(location.search).get('room'); } catch (_) {}

    if (urlRoom) { RoomSync.joinRoom(urlRoom); return; }

    var saved = readPersist();
    if (saved && saved.roomId) {
      if (saved.role === 'viewer') RoomSync.joinRoom(saved.roomId);
      else if (saved.role === 'host') RoomSync._rehost(saved.roomId);
    }
  }

  if (document.readyState === 'complete') autostart();
  else window.addEventListener('load', autostart);
})();
