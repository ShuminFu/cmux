(function () {
  'use strict';
  var root = document.documentElement;
  var svg = document.getElementById('diagram');
  var stage = document.getElementById('stage');
  var vbW = parseFloat(svg.getAttribute('data-vb-w'));
  var vbH = parseFloat(svg.getAttribute('data-vb-h'));
  var base = { x: 0, y: 0, w: vbW, h: vbH };
  var view = { x: 0, y: 0, w: vbW, h: vbH };
  var TOKENS = ['--bg','--panel','--panel-2','--ink','--muted','--line','--grid','--accent','--focus','--mono',
    '--k-frontend-fill','--k-frontend-stroke','--k-backend-fill','--k-backend-stroke','--k-database-fill','--k-database-stroke',
    '--k-cloud-fill','--k-cloud-stroke','--k-security-fill','--k-security-stroke','--k-messagebus-fill','--k-messagebus-stroke',
    '--k-external-fill','--k-external-stroke','--r-default','--r-emphasis','--r-security','--r-dashed','--r-main','--r-error',
    '--band-lane','--band-exception','--band-phase','--phase-emphasis','--phase-dashed'];

  // ---- theme -------------------------------------------------------------
  function applyTheme(choice) {
    if (choice === 'light' || choice === 'dark') root.setAttribute('data-theme', choice);
    else root.removeAttribute('data-theme');
    document.querySelectorAll('[data-theme-choice]').forEach(function (b) {
      b.setAttribute('aria-pressed', String(b.getAttribute('data-theme-choice') === choice));
    });
    try { localStorage.setItem('archify-rs-theme', choice); } catch (e) {}
  }
  var forced = null;
  try { forced = new URLSearchParams(location.search).get('theme'); } catch (e) {}
  var stored = null;
  try { stored = localStorage.getItem('archify-rs-theme'); } catch (e) {}
  // A host that stamps data-theme on <html> (an embedding viewer) wins over
  // the default; an explicit reader choice on this page still wins over it.
  var hosted = root.getAttribute('data-theme');
  applyTheme(forced || stored || hosted || 'system');
  document.querySelectorAll('[data-theme-choice]').forEach(function (b) {
    b.addEventListener('click', function () { applyTheme(b.getAttribute('data-theme-choice')); });
  });

  // ---- pan / zoom --------------------------------------------------------
  function setView() {
    svg.setAttribute('viewBox', [view.x, view.y, view.w, view.h].map(function (v) { return Math.round(v * 100) / 100; }).join(' '));
    var z = document.getElementById('zoom-level');
    if (z) z.textContent = Math.round(base.w / view.w * 100) + '%';
  }
  function zoomAt(factor, cx, cy) {
    var nw = Math.min(base.w * 8, Math.max(base.w / 8, view.w / factor));
    var nh = nw * (base.h / base.w);
    var rx = cx == null ? 0.5 : cx;
    var ry = cy == null ? 0.5 : cy;
    view.x = view.x + (view.w - nw) * rx;
    view.y = view.y + (view.h - nh) * ry;
    view.w = nw; view.h = nh;
    setView();
  }
  svg.addEventListener('wheel', function (ev) {
    ev.preventDefault();
    var r = svg.getBoundingClientRect();
    zoomAt(ev.deltaY < 0 ? 1.12 : 1 / 1.12, (ev.clientX - r.left) / r.width, (ev.clientY - r.top) / r.height);
  }, { passive: false });
  var drag = null;
  svg.addEventListener('pointerdown', function (ev) {
    if (ev.target.closest && ev.target.closest('a')) return;
    drag = { x: ev.clientX, y: ev.clientY, vx: view.x, vy: view.y };
    svg.classList.add('dragging');
    try { svg.setPointerCapture(ev.pointerId); } catch (e) {}
  });
  svg.addEventListener('pointermove', function (ev) {
    if (!drag) return;
    var r = svg.getBoundingClientRect();
    view.x = drag.vx - (ev.clientX - drag.x) * (view.w / r.width);
    view.y = drag.vy - (ev.clientY - drag.y) * (view.h / r.height);
    setView();
  });
  function endDrag() { drag = null; svg.classList.remove('dragging'); }
  svg.addEventListener('pointerup', endDrag);
  svg.addEventListener('pointercancel', endDrag);
  document.querySelectorAll('[data-zoom]').forEach(function (b) {
    b.addEventListener('click', function () {
      var k = b.getAttribute('data-zoom');
      if (k === '+') zoomAt(1.25);
      else if (k === '-') zoomAt(1 / 1.25);
      else { view = { x: base.x, y: base.y, w: base.w, h: base.h }; setView(); }
    });
  });

  // ---- guided views ------------------------------------------------------
  var note = document.getElementById('view-note');
  function setFocus(ids) {
    var set = {};
    ids.forEach(function (id) { set[id] = true; });
    var on = ids.length > 0;
    svg.classList.toggle('focus-mode', on);
    svg.querySelectorAll('.node').forEach(function (n) { n.classList.toggle('is-focus', !!set[n.getAttribute('data-id')]); });
    svg.querySelectorAll('.route').forEach(function (p) {
      var f = !!(set[p.getAttribute('data-from')] && set[p.getAttribute('data-to')]);
      p.classList.toggle('is-focus', f);
      var lab = svg.querySelector('.rlabel[data-route="' + CSS.escape(p.getAttribute('data-id')) + '"]');
      if (lab) lab.classList.toggle('is-focus', f);
    });
  }
  document.querySelectorAll('[data-view]').forEach(function (b) {
    b.addEventListener('click', function () {
      document.querySelectorAll('[data-view]').forEach(function (o) { o.setAttribute('aria-pressed', 'false'); });
      b.setAttribute('aria-pressed', 'true');
      var focus = (b.getAttribute('data-focus') || '').split(',').filter(Boolean);
      setFocus(focus);
      if (note) note.textContent = b.getAttribute('data-note') || '';
    });
  });

  // ---- search ------------------------------------------------------------
  var search = document.getElementById('search');
  if (search) {
    search.addEventListener('input', function () {
      var q = search.value.trim().toLowerCase();
      svg.classList.toggle('search-mode', q.length > 0);
      svg.querySelectorAll('.node').forEach(function (n) {
        var hay = (n.getAttribute('data-label') || '').toLowerCase();
        n.classList.toggle('hit', q.length > 0 && hay.indexOf(q) >= 0);
      });
    });
  }

  // ---- present -----------------------------------------------------------
  var present = document.getElementById('btn-present');
  if (present) {
    present.addEventListener('click', function () {
      var on = document.body.classList.toggle('present');
      present.setAttribute('aria-pressed', String(on));
      if (on && root.requestFullscreen) { root.requestFullscreen().catch(function () {}); }
      else if (!on && document.fullscreenElement && document.exitFullscreen) { document.exitFullscreen().catch(function () {}); }
      fitReader();
    });
  }

  // ---- export ------------------------------------------------------------
  var menu = document.querySelector('.menu');
  var exportBtn = document.getElementById('btn-export');
  if (exportBtn && menu) {
    exportBtn.addEventListener('click', function (ev) { ev.stopPropagation(); menu.classList.toggle('open'); });
    document.addEventListener('click', function () { menu.classList.remove('open'); });
  }
  function standaloneSvg() {
    var clone = svg.cloneNode(true);
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
    clone.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');
    clone.setAttribute('viewBox', [base.x, base.y, base.w, base.h].join(' '));
    clone.setAttribute('width', String(base.w));
    clone.setAttribute('height', String(base.h));
    clone.classList.remove('focus-mode', 'search-mode');
    var cs = getComputedStyle(root);
    var vars = TOKENS.map(function (t) { return t + ':' + cs.getPropertyValue(t).trim() + ';'; }).join('');
    var style = document.createElementNS('http://www.w3.org/2000/svg', 'style');
    style.textContent = ':root,svg{' + vars + '}' + document.getElementById('archify-css').textContent;
    clone.insertBefore(style, clone.firstChild);
    var bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    bg.setAttribute('width', '100%'); bg.setAttribute('height', '100%'); bg.setAttribute('fill', cs.getPropertyValue('--panel').trim());
    clone.insertBefore(bg, style.nextSibling);
    return new XMLSerializer().serializeToString(clone);
  }
  function download(blob, name) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a);
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 500);
  }
  var slug = (document.title || 'diagram').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'diagram';
  document.querySelectorAll('[data-export]').forEach(function (b) {
    b.addEventListener('click', function () {
      var kind = b.getAttribute('data-export');
      var text = standaloneSvg();
      if (kind === 'svg') { download(new Blob([text], { type: 'image/svg+xml' }), slug + '.svg'); return; }
      var img = new Image();
      var url = URL.createObjectURL(new Blob([text], { type: 'image/svg+xml' }));
      img.onload = function () {
        var scale = 2;
        var c = document.createElement('canvas');
        c.width = Math.round(base.w * scale); c.height = Math.round(base.h * scale);
        var ctx = c.getContext('2d');
        ctx.drawImage(img, 0, 0, c.width, c.height);
        URL.revokeObjectURL(url);
        c.toBlob(function (blob) { if (blob) download(blob, slug + '@2x.png'); }, 'image/png');
      };
      img.src = url;
    });
  });

  // ---- adaptive reader width + instrumentation ---------------------------
  function outerHeightExceptStage() {
    var total = 0;
    var els = document.querySelectorAll('.topbar, .views, #view-note, .cards, .foot, .stage-tools');
    els.forEach(function (el) {
      if (!el.offsetParent && getComputedStyle(el).display === 'none') return;
      var cs = getComputedStyle(el);
      total += el.getBoundingClientRect().height + parseFloat(cs.marginTop || 0) + parseFloat(cs.marginBottom || 0);
    });
    var app = getComputedStyle(document.querySelector('.app'));
    total += parseFloat(app.paddingTop || 0) + parseFloat(app.paddingBottom || 0) + 12 /* reader margin */ + 26 /* stage padding */;
    return total;
  }
  function fitReader() {
    var minW = 960, maxW = Math.min(1440, window.innerWidth - 32);
    var aspect = vbW / vbH;
    for (var pass = 0; pass < 3; pass++) {
      var avail = window.innerHeight - outerHeightExceptStage() - 2;
      var byHeight = avail * aspect + 26;
      var w = Math.max(minW, Math.min(maxW, byHeight));
      document.documentElement.style.setProperty('--reader-width', Math.floor(w) + 'px');
    }
    instrument();
  }
  function instrument() {
    var r = svg.getBoundingClientRect();
    var scale = r.width / vbW;
    var min = Infinity;
    svg.querySelectorAll('.node text').forEach(function (t) {
      var fs = parseFloat(t.getAttribute('font-size') || '0');
      if (fs > 0) min = Math.min(min, fs * scale);
    });
    root.setAttribute('data-inner-w', String(window.innerWidth));
    root.setAttribute('data-inner-h', String(window.innerHeight));
    root.setAttribute('data-scroll-w', String(root.scrollWidth));
    root.setAttribute('data-scroll-h', String(root.scrollHeight));
    root.setAttribute('data-diagram-w', String(Math.round(r.width)));
    root.setAttribute('data-min-text-px', isFinite(min) ? min.toFixed(3) : '');
  }
  window.addEventListener('resize', fitReader);
  if (document.fonts && document.fonts.ready) { document.fonts.ready.then(fitReader); }
  fitReader();
  requestAnimationFrame(function () { requestAnimationFrame(fitReader); });
  setView();
})();
