// ---- FIREBASE_CONFIG: paste your real config here ----
  const firebaseConfig = {
    apiKey: "AIzaSyB7ovDN0lvwYZ872cFCxF_yI8BAFbm6xpw",
    authDomain: "coffee-drop-monitor.firebaseapp.com",
    projectId: "coffee-drop-monitor",
    storageBucket: "coffee-drop-monitor.firebasestorage.app",
    messagingSenderId: "578440481584",
    appId: "1:578440481584:web:97463ed94ebc3f976696f5",
    measurementId: "G-76KG6PQLNF"
  };

  import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.3/firebase-app.js";
  import { getFirestore, collection, getDocs, doc, setDoc, deleteDoc, updateDoc, serverTimestamp, arrayUnion, query, orderBy, limit, startAfter } from "https://www.gstatic.com/firebasejs/10.12.3/firebase-firestore.js";
  import { getAuth, onAuthStateChanged, GoogleAuthProvider, signInWithPopup, signOut as fbSignOut, signInWithEmailAndPassword, createUserWithEmailAndPassword } from "https://www.gstatic.com/firebasejs/10.12.3/firebase-auth.js";

  const app = initializeApp(firebaseConfig);
  const db  = getFirestore(app);
  const auth = getAuth(app);
  const provider = new GoogleAuthProvider();

  const esc = s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  const qEl = document.getElementById('q');
  const roEl = document.getElementById('roaster');
  const coEl = document.getElementById('country');
  const stEl = document.getElementById('stock');
  const sortEl = document.getElementById('sort');
  const groupEl = document.getElementById('groupBy');
  // Sidebar controls
  const menuToggle = document.getElementById('menuToggle');
  const sidebar = document.getElementById('sidebar');
  const sidebarBackdrop = document.getElementById('sidebarBackdrop');
  const sidebarClose = document.getElementById('sidebarClose');
  const sbHide = document.getElementById('sb_hideSoldOut');
  const sbOnlyTried = document.getElementById('sb_onlyTried');
  const gridIn = document.getElementById('grid-in');
  const gridOut = document.getElementById('grid-out');
  const roasterColors = new Map();
  const stamp = document.getElementById('stamp');
  const signInBtn = document.getElementById('signIn');
  const signOutBtn = document.getElementById('signOut');
  const userInfo = document.getElementById('userInfo');
  const avatarBtn = document.getElementById('avatarBtn');
  const authMenu = document.getElementById('authMenu');
  const avatarLarge = document.getElementById('avatarLarge');
  const menuUserName = document.getElementById('menuUserName');
  const menuUserEmail = document.getElementById('menuUserEmail');
  const menuGoogle = document.getElementById('menuGoogle');
  const menuSignOut = document.getElementById('menuSignOut');
  const menuBackdrop = document.getElementById('menuBackdrop');
  const authEmail = document.getElementById('authEmail');
  const authPass = document.getElementById('authPass');
  const emailSignIn = document.getElementById('emailSignIn');
  const emailRegister = document.getElementById('emailRegister');
  
  // Section toggle elements
  const headerIn = document.getElementById('header-in');
  const headerOut = document.getElementById('header-out');
  const secIn = document.getElementById('sec-in');
  const secOut = document.getElementById('sec-out');
  const prevIn = document.getElementById('prev-in');
  const nextIn = document.getElementById('next-in');
  const prevOut = document.getElementById('prev-out');
  const nextOut = document.getElementById('next-out');

  const newBanner = document.getElementById('newBanner');
  const newCount  = document.getElementById('newCount');
  const newChips  = document.getElementById('newChips');
  const PREFERS_REDUCED_MOTION = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const backdrop = document.getElementById('backdrop');
  const mTitle = document.getElementById('m_title');
  const mRoaster = document.getElementById('m_roaster');
  const mPrice = document.getElementById('m_price');
  const mCountry = document.getElementById('m_country');
  const mProcess = document.getElementById('m_process');
  const mProfile = document.getElementById('m_profile');
  const mNotes = document.getElementById('m_notes');
  const mFirst = document.getElementById('m_first');
  const mLast = document.getElementById('m_last');
  const mLink = document.getElementById('m_link');
  const mClose = document.getElementById('m_close');
  const mPrev = document.getElementById('m_prev');
  const mNext = document.getElementById('m_next');
  const mTried = document.getElementById('m_tried');
  const mPurchased = document.getElementById('m_purchased');
  const mPurchase = document.getElementById('m_purchase');
  const mTry = document.getElementById('m_try');
  const tryControls = document.getElementById('tryControls');
  const mTryRating = document.getElementById('m_try_rating');
  const mTryNotes = document.getElementById('m_try_notes');

  const modalEl = document.querySelector('#backdrop .modal');
  let ITEMS = [];
  let CARDS = [];
  let LAST_DOC = null;
  let NEW_IDS = [];            // ids provided via ?ids=...
  let CURRENT_NEW_INDEX = -1;  // pointer within NEW_IDS
  let TRIED_URLS = new Set();
  let PURCHASED_URLS = new Set();
  let CURRENT_USER = null; // firebase user
  // UI state toggles
  let HIDE_SOLD_OUT = false;
  let ONLY_TRIED = false;

  function byId(id){ return ITEMS.find(x=>x.id===id); }

  function setChip(el, text){
    const t = (text||'').toString().trim();
    if (t){ el.textContent = t; el.style.display = 'inline-flex'; }
    else { el.textContent = ''; el.style.display = 'none'; }
  }

  // Robustly parse 'YYYY-MM-DD HH:MM[:SS][+/-ZZZZ]' or ISO strings to epoch ms
  function parseTime(s){
    if (!s) return 0;
    const t = Date.parse(s);
    if (!Number.isNaN(t)) return t;
    const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?(?:([+-])(\d{2})(\d{2}))?$/);
    if (!m) return 0;
    const [, Y, Mo, D, h, mi, se='00', sign, offH='00', offM='00'] = m;
    const utc = Date.UTC(+Y, +Mo-1, +D, +h, +mi, +se);
    if (!sign) return utc;
    const offset = ((+offH)*60 + (+offM)) * 60000;
    return sign === '+' ? utc - offset : utc + offset;
  }

  // --- Carousel helpers ---
  let updateInButtons = null;
  let updateOutButtons = null;

  function setupCarousel(container, prevBtn, nextBtn){
    if (!container || !prevBtn || !nextBtn) return () => {};
    const behavior = PREFERS_REDUCED_MOTION ? 'auto' : 'smooth';

    function updateButtons(){
      const maxScroll = Math.max(0, container.scrollWidth - container.clientWidth - 1);
      prevBtn.disabled = container.scrollLeft <= 0;
      nextBtn.disabled = container.scrollLeft >= maxScroll;
    }

    function scrollByStep(dir){
      const step = Math.max(200, Math.floor(container.clientWidth * 0.9));
      container.scrollBy({ left: dir * step, behavior });
    }

    // Prevent section header toggle when pressing controls
    prevBtn.addEventListener('click', (e)=>{ e.stopPropagation(); scrollByStep(-1); });
    nextBtn.addEventListener('click', (e)=>{ e.stopPropagation(); scrollByStep(1); });

    // Keyboard support when buttons focused
    ;[prevBtn, nextBtn].forEach(btn => {
      btn.addEventListener('keydown', (e)=>{
        if (e.key === 'ArrowLeft'){ e.preventDefault(); scrollByStep(-1); }
        else if (e.key === 'ArrowRight'){ e.preventDefault(); scrollByStep(1); }
      });
    });

    // Wheel: convert vertical scroll to horizontal for mouse wheels
    container.addEventListener('wheel', (e) => {
      const absX = Math.abs(e.deltaX);
      const absY = Math.abs(e.deltaY);
      const maxScroll = container.scrollWidth - container.clientWidth;
      if (maxScroll <= 0) return; // nothing to scroll
      // Let trackpads/native horizontal scroll work
      if (absX > absY) return;
      const atStart = container.scrollLeft <= 0;
      const atEnd = container.scrollLeft >= (maxScroll - 1);
      const goingLeft = e.deltaY < 0;
      const goingRight = e.deltaY > 0;
      // If at the edge and scrolling outward, allow page to scroll
      if ((goingLeft && atStart) || (goingRight && atEnd)) return;
      e.preventDefault();
      container.scrollLeft += e.deltaY;
    }, { passive: false });

    container.addEventListener('scroll', updateButtons, { passive: true });
    window.addEventListener('resize', updateButtons);
    // Init
    updateButtons();
    return updateButtons;
  }

  function openModal(item){
    if(!item) return;
    mTitle.textContent = item.title;
    setChip(mRoaster, item.roaster);
    setChip(mPrice, item.price);
    setChip(mCountry, item.country);
    setChip(mProcess, item.process);
    setChip(mProfile, item.profile);
    if ((item.notes||'').trim()){ mNotes.textContent = item.notes; mNotes.style.display='block'; }
    else { mNotes.textContent=''; mNotes.style.display='none'; }
    mFirst.textContent = item.first_seen || '';
    mLast.textContent = item.last_seen || '';
    mLink.href = item.url;
    // Tried & purchased chips
    if (mTried) setChip(mTried, item.tried ? 'Tried' : '');
    if (mPurchased) setChip(mPurchased, item.purchased ? 'Purchased' : '');
    // Tried controls & button visibility (require auth)
    if (CURRENT_USER && mTry && tryControls) {
      mTry.style.display = '';
      tryControls.style.display = '';
      mTry.textContent = item.tried ? 'Unmark tried' : 'Mark tried';
      mTry.setAttribute('aria-pressed', item.tried ? 'true' : 'false');
      mTry.onclick = async () => {
        try {
          mTry.disabled = true;
          await toggleTried(item);
        } finally {
          mTry.disabled = false;
        }
      };
    } else if (mTry && tryControls) {
      mTry.style.display = 'none';
      tryControls.style.display = 'none';
    }
    // Purchase toggle button
    if (mPurchase) {
      mPurchase.style.display = '';
      mPurchase.textContent = item.purchased ? 'Unmark purchased' : 'Mark purchased';
      mPurchase.setAttribute('aria-pressed', item.purchased ? 'true' : 'false');
      mPurchase.onclick = async () => {
        try {
          mPurchase.disabled = true;
          await togglePurchased(item);
        } finally {
          mPurchase.disabled = false;
        }
      };
    }
    backdrop.style.display='flex';
    modalEl.focus();
    document.body.style.overflow='hidden';
    history.replaceState(null,'',`?id=${item.id}` + (NEW_IDS.length ? `&ids=${NEW_IDS.join(',')}` : ''));
    updatePrevNext(item.id);
  }

  function closeModal(){ backdrop.style.display='none'; document.body.style.overflow=''; }

  function updatePrevNext(currentId){
    if (!NEW_IDS.length){
      mPrev.style.display = 'none';
      mNext.style.display = 'none';
      return;
    }
    mPrev.style.display = '';
    mNext.style.display = '';
    CURRENT_NEW_INDEX = NEW_IDS.indexOf(currentId);
    mPrev.disabled = CURRENT_NEW_INDEX <= 0;
    mNext.disabled = CURRENT_NEW_INDEX === -1 || CURRENT_NEW_INDEX >= NEW_IDS.length - 1;
  }

  function jumpToId(id){
    const it = byId(id);
    if (it){
      const el = CARDS.find(c=>c.dataset.id===id);
      if (el) el.scrollIntoView({behavior: PREFERS_REDUCED_MOTION ? 'auto' : 'smooth', block:'center'});
      openModal(it);
    }
  }

  async function togglePurchased(item){
    if (!CURRENT_USER) { alert('Please sign in to mark purchased.'); return; }
    const isPurchased = PURCHASED_URLS.has(item.url);
    try {
      const pref = doc(db, 'users', CURRENT_USER.uid, 'purchased', item.id);
      if (isPurchased) {
        await deleteDoc(pref);
        PURCHASED_URLS.delete(item.url);
        item.purchased = false;
      } else {
        await setDoc(pref, {
          url: item.url,
          roaster: item.roaster || '',
          title: item.title || '',
          last_purchased_on: serverTimestamp(),
        }, { merge: true });
        PURCHASED_URLS.add(item.url);
        item.purchased = true;
      }
      // Update modal UI
      if (mPurchased) setChip(mPurchased, item.purchased ? 'Purchased' : '');
      if (mPurchase) {
        mPurchase.textContent = item.purchased ? 'Unmark purchased' : 'Mark purchased';
        mPurchase.setAttribute('aria-pressed', item.purchased ? 'true' : 'false');
      }
      // Update card dataset and tag
      const card = CARDS.find(c => c.dataset.id === item.id);
      if (card) {
        card.dataset.purchased = item.purchased ? '1' : '0';
        const tags = card.querySelector('.tags');
        if (tags) {
          let chip = tags.querySelector("[data-chip='purchased']");
          if (item.purchased) {
            if (!chip) {
              chip = document.createElement('span');
              chip.className = 'chip';
              chip.title = 'Purchased';
              chip.setAttribute('data-chip', 'purchased');
              chip.textContent = 'Purchased';
              tags.appendChild(chip);
            } else {
              chip.style.display = 'inline-flex';
            }
          } else if (chip) {
            chip.style.display = 'none';
          }
        }
      }
    } catch (e) {
      console.error('Purchase toggle failed:', e);
      alert('Failed to update purchased status. Check Firestore rules/permissions.');
    }
  }

  async function toggleTried(item){
    if (!CURRENT_USER) { alert('Please sign in to mark tried.'); return; }
    const isTried = TRIED_URLS.has(item.url);
    try {
      const tref = doc(db, 'users', CURRENT_USER.uid, 'tried', item.id);
      if (isTried) {
        await deleteDoc(tref);
        TRIED_URLS.delete(item.url);
        item.tried = false;
      } else {
        const ratingVal = (mTryRating && mTryRating.value) ? Number(mTryRating.value) : undefined;
        const notesVal = (mTryNotes && mTryNotes.value || '').trim();
        const payload = {
          url: item.url,
          roaster: item.roaster || '',
          title: item.title || '',
          last_tried_on: serverTimestamp(),
        };
        if (notesVal) payload.last_notes = notesVal;
        if (!Number.isNaN(ratingVal) && ratingVal) payload.last_rating = ratingVal;
        await setDoc(tref, payload, { merge: true });
        // Append to history using arrayUnion (best effort)
        try {
          await updateDoc(tref, { history: arrayUnion({ tried_on: serverTimestamp(), ...(notesVal ? { notes: notesVal } : {}), ...(ratingVal ? { rating: ratingVal } : {}) }) });
        } catch (_) {}
        TRIED_URLS.add(item.url);
        item.tried = true;
      }
      // Update modal UI
      if (mTried) setChip(mTried, item.tried ? 'Tried' : '');
      if (mTry) {
        mTry.textContent = item.tried ? 'Unmark tried' : 'Mark tried';
        mTry.setAttribute('aria-pressed', item.tried ? 'true' : 'false');
      }
      // Update card
      const card = CARDS.find(c => c.dataset.id === item.id);
      if (card) {
        card.dataset.tried = item.tried ? '1' : '0';
        const tags = card.querySelector('.tags');
        if (tags) {
          let chip = tags.querySelector("[data-chip='tried']");
          if (item.tried) {
            if (!chip) {
              chip = document.createElement('span');
              chip.className = 'chip ok';
              chip.title = 'Tried';
              chip.setAttribute('data-chip', 'tried');
              chip.textContent = 'Tried';
              tags.appendChild(chip);
            } else {
              chip.style.display = 'inline-flex';
            }
          } else if (chip) {
            chip.style.display = 'none';
          }
        }
      }
      apply();
      resort();
    } catch (e) {
      console.error('Tried toggle failed:', e);
      alert('Failed to update tried status. Check Firestore rules/permissions.');
    }
  }

  backdrop.addEventListener('click', e=>{ if(e.target===backdrop) closeModal(); });
  mClose.addEventListener('click', closeModal);
  mPrev.addEventListener('click', ()=>{
    if (CURRENT_NEW_INDEX > 0) jumpToId(NEW_IDS[CURRENT_NEW_INDEX - 1]);
  });
  mNext.addEventListener('click', ()=>{
    if (CURRENT_NEW_INDEX !== -1 && CURRENT_NEW_INDEX < NEW_IDS.length - 1)
      jumpToId(NEW_IDS[CURRENT_NEW_INDEX + 1]);
  });
  window.addEventListener('keydown', (e)=>{
    if (backdrop.style.display === 'flex'){
      if (e.key === 'ArrowLeft') mPrev.click();
      if (e.key === 'ArrowRight') mNext.click();
      if (e.key === 'Escape') mClose.click();
    }
  });

  function apply(){
    const qs=(qEl.value||'').toLowerCase();
    const rr=roEl.value; const cc=(coEl.value||'').toLowerCase(); const ss=stEl.value;
    const hide = HIDE_SOLD_OUT;
    for(const c of CARDS){
      const mText = c.dataset.title.includes(qs) || c.dataset.price.includes(qs) || c.dataset.notes.includes(qs) || (c.dataset.process||'').includes(qs) || (c.dataset.profile||'').includes(qs);
      const mRo = !rr || c.dataset.roaster===rr;
      const mCo = !cc || c.dataset.country===cc;
      const mSt = !ss || c.dataset.stock===ss;
      const mTr = !ONLY_TRIED || c.dataset.tried === '1';
      const mHide = !(hide && c.dataset.stock==='out');
      c.style.display=(mText&&mRo&&mCo&&mSt&&mTr&&mHide)?'flex':'none';
    }
  }

  function getSorter(mode) {
    return (a, b) => {
      const la = parseFloat(a.dataset.timelast || '0');
      const lb = parseFloat(b.dataset.timelast || '0');
      if (mode === 'stock' || mode === 'last') {
        if (lb !== la) return lb - la;
        return (a.dataset.title || '').localeCompare(b.dataset.title || '');
      }
      if (mode === 'roaster') {
        const rc = (a.dataset.roaster || '').localeCompare(b.dataset.roaster || '');
        if (rc) return rc;
        return lb - la;
      }
      if (mode === 'country') {
        const cc = (a.dataset.country || '').localeCompare(b.dataset.country || '');
        if (cc) return cc;
        return lb - la;
      }
      if (mode === 'title') {
        return (a.dataset.title || '').localeCompare(b.dataset.title || '');
      }
      return 0;
    };
  }

  function renderUngrouped(container, cards) {
    const frag = document.createDocumentFragment();
    for (const c of cards) { frag.appendChild(c); }
    container.appendChild(frag);
  }

  function renderGrouped(container, cards, groupMode, sortMode) {
    const groups = new Map();
    for (const c of cards) {
      let key = '';
      let label = '';
      if (groupMode === 'roaster') { key = (c.dataset.roaster || ''); label = c.dataset.roastername || key || 'Unknown'; }
      else if (groupMode === 'country') { key = (c.dataset.country || ''); label = c.dataset.countryname || key || 'Unknown'; }
      const k = key || 'zzz~unknown'; // for sorting empty keys last
      if (!groups.has(k)) groups.set(k, { label, items: [] });
      groups.get(k).items.push(c);
    }

    const sortedKeys = [...groups.keys()].sort((a, b) => a.localeCompare(b));
    const wrapFrag = document.createDocumentFragment();
    for (const k of sortedKeys) {
      const { label, items } = groups.get(k);
      const innerSortMode = (sortMode === groupMode) ? 'last' : sortMode;
      items.sort(getSorter(innerSortMode));

      const wrap = document.createElement('div'); wrap.className = 'group';
      const h = document.createElement('div'); h.className = 'group-title'; h.textContent = label;
      const g = document.createElement('div'); g.className = 'grid';
      const f = document.createDocumentFragment();
      for (const c of items) { f.appendChild(c); }
      g.appendChild(f);
      wrap.appendChild(h);
      wrap.appendChild(g);
      wrapFrag.appendChild(wrap);
    }
    container.appendChild(wrapFrag);
  }

  function resort() {
    const sortMode = (sortEl && sortEl.value) || 'stock';
    const groupMode = (groupEl && groupEl.value) || 'none';

    const visible = CARDS.filter(c => c.style.display !== 'none');
    const inCards = visible.filter(c => c.dataset.stock === 'in');
    const outCards = visible.filter(c => c.dataset.stock === 'out');

    const sorter = getSorter(sortMode);
    inCards.sort(sorter);
    outCards.sort(sorter);

    if (gridIn) gridIn.innerHTML = '';
    if (gridOut) gridOut.innerHTML = '';

    if (groupMode === 'none') {
      renderUngrouped(gridIn, inCards);
      renderUngrouped(gridOut, outCards);
    } else {
      renderGrouped(gridIn, inCards, groupMode, sortMode);
      renderGrouped(gridOut, outCards, groupMode, sortMode);
    }

    if (secIn) secIn.style.display = inCards.length ? '' : 'none';
    if (secOut) secOut.style.display = outCards.length ? '' : 'none';

    // Update carousel buttons after DOM changes
    if (updateInButtons) updateInButtons();
    if (updateOutButtons) updateOutButtons();
  }

  (async function init(){
    // Load UI preferences
    try {
      const pref = (k, d) => localStorage.getItem(k) ?? d;
      if (qEl) qEl.value = pref('pref:q', '');
      if (roEl) roEl.value = pref('pref:roaster', '');
      if (coEl) coEl.value = pref('pref:country', '');
      if (stEl) stEl.value = pref('pref:stock', '');
      if (sortEl) sortEl.value = pref('pref:sort', 'stock');
      if (groupEl) groupEl.value = pref('pref:group', 'none');
      HIDE_SOLD_OUT = pref('pref:hideSold', '0') === '1';
      ONLY_TRIED = pref('pref:onlyTried', '0') === '1';
    } catch (_) {}
    const params = new URLSearchParams(location.search);
    const collectionName = params.get('collection') || 'coffees';
    // Pagination-aware initial fetch
    async function fetchPage(limitCount = 60){
      let q = query(collection(db, collectionName), orderBy('last_seen', 'desc'), limit(limitCount));
      if (LAST_DOC) q = query(collection(db, collectionName), orderBy('last_seen', 'desc'), startAfter(LAST_DOC), limit(limitCount));
      const snap = await getDocs(q);
      if (!snap.empty) LAST_DOC = snap.docs[snap.docs.length - 1];
      return snap.docs.map(d => ({ id:d.id, ...d.data() }));
    }
    const items = await fetchPage(60);

    // Auth + per-user tried/purchased
    function updateAuthUI(){
      if (userInfo) userInfo.textContent = CURRENT_USER ? (CURRENT_USER.displayName || CURRENT_USER.email || '') : '';
      if (mPurchase) mPurchase.style.display = CURRENT_USER ? '' : 'none';
      if (menuUserName) menuUserName.textContent = CURRENT_USER ? (CURRENT_USER.displayName || '') : 'Guest';
      if (menuUserEmail) menuUserEmail.textContent = CURRENT_USER ? (CURRENT_USER.email || '') : '';
      if (menuGoogle) menuGoogle.style.display = CURRENT_USER ? 'none' : '';
      if (menuSignOut) menuSignOut.style.display = CURRENT_USER ? '' : 'none';
      if (avatarBtn) {
        const seed = (CURRENT_USER && (CURRENT_USER.uid || CURRENT_USER.email || 'user')) || 'guest';
        const hue = Math.abs([...seed].reduce((a,c)=>a + c.charCodeAt(0)*7, 0)) % 360;
        avatarBtn.style.background = `conic-gradient(from 0deg, hsl(${hue},70%,40%), hsl(${(hue+60)%360},70%,40%), hsl(${(hue+120)%360},70%,40%))`;
        avatarBtn.style.borderColor = 'rgba(134,225,160,0.25)';
      }
      if (avatarLarge) {
        const seed = (CURRENT_USER && (CURRENT_USER.uid || CURRENT_USER.email || 'user')) || 'guest';
        const hue = Math.abs([...seed].reduce((a,c)=>a + c.charCodeAt(0)*7, 0)) % 360;
        avatarLarge.style.background = `conic-gradient(from 0deg, hsl(${hue},70%,40%), hsl(${(hue+60)%360},70%,40%), hsl(${(hue+120)%360},70%,40%))`;
        avatarLarge.style.border = '1px solid rgba(134,225,160,0.25)';
      }
    }
    async function loadUserSets(){
      let tset = new Set();
      let pset = new Set();
      if (CURRENT_USER) {
        try {
          const tsnap = await getDocs(collection(db, 'users', CURRENT_USER.uid, 'tried'));
          tsnap.docs.forEach(docu => { const data = docu.data() || {}; const u = data.url; if (u) tset.add(u); });
        } catch (e) { console.warn('Tried load failed:', e); }
        try {
          const psnap = await getDocs(collection(db, 'users', CURRENT_USER.uid, 'purchased'));
          psnap.docs.forEach(docu => { const data = docu.data() || {}; const u = data.url; if (u) pset.add(u); });
        } catch (e) { console.warn('Purchased load failed:', e); }
      }
      TRIED_URLS = tset;
      PURCHASED_URLS = pset;
      // Reflect onto ITEMS and CARDS
      for (const it of ITEMS) {
        it.tried = TRIED_URLS.has(it.url);
        it.purchased = PURCHASED_URLS.has(it.url);
      }
      for (const card of CARDS) {
        const it = ITEMS.find(x => x.id === card.dataset.id);
        if (!it) continue;
        card.dataset.tried = it.tried ? '1' : '0';
        card.dataset.purchased = it.purchased ? '1' : '0';
        const tags = card.querySelector('.tags');
        if (tags) {
          // Tried chip
          let tchip = tags.querySelector("[data-chip='tried']");
          if (it.tried) {
            if (!tchip) { tchip = document.createElement('span'); tchip.className='chip ok'; tchip.title='Tried'; tchip.setAttribute('data-chip','tried'); tchip.textContent='Tried'; tags.appendChild(tchip); }
            else { tchip.style.display='inline-flex'; }
          } else if (tchip) { tchip.style.display='none'; }
          // Purchased chip
          let pchip = tags.querySelector("[data-chip='purchased']");
          if (it.purchased) {
            if (!pchip) { pchip = document.createElement('span'); pchip.className='chip'; pchip.title='Purchased'; pchip.setAttribute('data-chip','purchased'); pchip.textContent='Purchased'; tags.appendChild(pchip); }
            else { pchip.style.display='inline-flex'; }
          } else if (pchip) { pchip.style.display='none'; }
        }
      }
      const gen = new Date().toISOString().slice(0,16).replace('T',' ');
      stamp.textContent = `Loaded ${gen} • ${ITEMS.length} items • ${TRIED_URLS.size} tried • ${PURCHASED_URLS.size} purchased`;
      apply();
      resort();
    }

    // Initially no per-user flags until auth resolves
    for (const it of items) { it.tried = false; it.purchased = false; }
    ITEMS = items;
    const gen = new Date().toISOString().slice(0,16).replace('T',' ');
    stamp.textContent = `Loaded ${gen} • ${items.length} items`;

    // filters
    const roasters = [...new Set(items.map(x=>x.roaster))].sort();
    for(const r of roasters){ const o=document.createElement('option'); o.textContent=r; roEl.appendChild(o); }
    const countries = [...new Set(items.map(x=>x.country).filter(Boolean))].sort();
    for(const c of countries){ const o=document.createElement('option'); o.textContent=c; coEl.appendChild(o); }

    // cards
    const frag = document.createDocumentFragment();
    for(const it of items){
      const a = document.createElement('a');
      a.className='card';
      a.href = it.url; a.target='_blank'; a.rel='noopener';
      a.dataset.id = it.id;
      a.dataset.roaster = it.roaster || '';
      a.dataset.roastername = it.roaster || '';
      a.dataset.title   = String(it.title ?? '').toLowerCase();
      a.dataset.price   = String(it.price ?? '').toLowerCase();
      a.dataset.notes   = String(it.notes ?? '').toLowerCase();
      a.dataset.country = String(it.country ?? '').toLowerCase();
      a.dataset.countryname = it.country || '';
      a.dataset.process = String(it.process ?? '').toLowerCase();
      a.dataset.profile = String(it.profile ?? '').toLowerCase();
      a.dataset.stock   = it.in_stock? 'in':'out';
            a.dataset.timelast = String(parseTime(it.last_seen));
      a.dataset.tried = it.tried ? '1' : '0';
      a.dataset.purchased = it.purchased ? '1' : '0';

      const roasterName = it.roaster || 'Unknown';
      if (!roasterColors.has(roasterName)) {
        const hash = [...roasterName].reduce((acc, char) => char.charCodeAt(0) + ((acc << 5) - acc), 0);
        const hue = hash % 360;
        roasterColors.set(roasterName, {
          solid: `hsl(${hue}, 85%, 70%)`,
          border: `hsl(${hue}, 70%, 55%)`,
          bg: `hsl(${hue}, 75%, 20%)`,
        });
      }
      const colors = roasterColors.get(roasterName);
      a.style.setProperty('--roaster-solid', colors.solid);
      a.style.setProperty('--roaster-border', colors.border);
      a.style.setProperty('--roaster-bg', colors.bg);

      a.innerHTML = `<div class="card-content">
        <div class='row'><div class='chip roaster'>${esc(it.roaster||'')}</div><div class='chip ${it.in_stock?'ok':'no'}'>${it.in_stock?'In stock':'Sold out'}</div></div>
        <div class='title-line'>${esc(it.title||'')}</div>
        ${it.image ? `<img src="${esc(it.image)}" alt="${esc(it.title||'Coffee image')}" style="width:100%; max-height:180px; object-fit:cover; border-radius:12px; border:1px solid rgba(38,42,53,.3)" />` : ''}
        <div class='tags'>
          ${it.country ? `<span class='chip' title='Origin'>${esc(it.country)}</span>` : ''}
          ${it.process ? `<span class='chip' title='Process'>${esc(it.process)}</span>` : ''}
          ${it.profile ? `<span class='chip' title='Profile'>${esc(it.profile)}</span>` : ''}
          ${it.tried ? `<span class='chip ok' title='Tried' data-chip='tried'>Tried</span>` : ''}
          ${it.purchased ? `<span class='chip' title='Purchased' data-chip='purchased'>Purchased</span>` : ''}
        </div>
        <div class='muted'>${esc(it.price||'')}</div>
        <div class='row small'><div>First: ${esc(it.first_seen||'')}</div><div>Last: ${esc(it.last_seen||'')}</div></div>
      </div>`;
      const roasterEl = a.querySelector('.row .chip');
      if (roasterEl) roasterEl.title = (it.roaster||'');
      const titleEl = a.querySelector('.title-line');
      if (titleEl) titleEl.title = (it.title||'');
      a.addEventListener('click', (e)=>{ e.preventDefault(); openModal(it); });
      frag.appendChild(a);
      CARDS.push(a);
    }
    // Initial mounting into sections happens via resort()

    qEl.addEventListener('input', ()=>{ apply(); resort(); });
    roEl.addEventListener('change', ()=>{ apply(); resort(); });
    coEl.addEventListener('change', ()=>{ apply(); resort(); });
    stEl.addEventListener('change', ()=>{ apply(); resort(); });
    if (sortEl) sortEl.addEventListener('change', resort);
    if (groupEl) groupEl.addEventListener('change', resort);
    // Persist preferences
    function savePrefs(){
      try {
        localStorage.setItem('pref:q', qEl.value||'');
        localStorage.setItem('pref:roaster', roEl.value||'');
        localStorage.setItem('pref:country', coEl.value||'');
        localStorage.setItem('pref:stock', stEl.value||'');
        localStorage.setItem('pref:sort', (sortEl && sortEl.value) || 'stock');
        localStorage.setItem('pref:group', (groupEl && groupEl.value) || 'none');
        localStorage.setItem('pref:hideSold', HIDE_SOLD_OUT ? '1' : '0');
        localStorage.setItem('pref:onlyTried', ONLY_TRIED ? '1' : '0');
      } catch (_) {}
    }
    qEl.addEventListener('input', savePrefs);
    roEl.addEventListener('change', savePrefs);
    coEl.addEventListener('change', savePrefs);
    stEl.addEventListener('change', savePrefs);
    if (sortEl) sortEl.addEventListener('change', savePrefs);
    if (groupEl) groupEl.addEventListener('change', savePrefs);

    // Sidebar interactions
    function setMenuExpanded(expanded){
      if (!menuToggle) return;
      menuToggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    }
    function openSidebar(){
      if (!sidebar) return;
      sidebar.classList.add('open');
      if (sidebarBackdrop) sidebarBackdrop.hidden = false;
      if (sidebar) sidebar.setAttribute('aria-hidden','false');
      setMenuExpanded(true);
    }
    function closeSidebar(){
      if (!sidebar) return;
      sidebar.classList.remove('open');
      if (sidebarBackdrop) sidebarBackdrop.hidden = true;
      if (sidebar) sidebar.setAttribute('aria-hidden','true');
      setMenuExpanded(false);
    }
    if (menuToggle) menuToggle.addEventListener('click', () => {
      if (sidebar && sidebar.classList.contains('open')) closeSidebar(); else openSidebar();
    });
    if (sidebarBackdrop) sidebarBackdrop.addEventListener('click', closeSidebar);
    if (sidebarClose) sidebarClose.addEventListener('click', closeSidebar);

    function updateToggle(btn, on, onText, offText){
      if (!btn) return;
      btn.setAttribute('aria-pressed', on ? 'true' : 'false');
      if (onText && offText) btn.textContent = on ? onText : offText;
    }
    updateToggle(sbHide, HIDE_SOLD_OUT, 'Show sold out', 'Hide sold out');
    updateToggle(sbOnlyTried, ONLY_TRIED, 'Show all', 'Show only tried');
    if (sbHide) sbHide.addEventListener('click', ()=>{
      HIDE_SOLD_OUT = !HIDE_SOLD_OUT;
      updateToggle(sbHide, HIDE_SOLD_OUT, 'Show sold out', 'Hide sold out');
      apply(); resort();
      try { localStorage.setItem('pref:hideSold', HIDE_SOLD_OUT ? '1' : '0'); } catch (_) {}
    });
    if (sbOnlyTried) sbOnlyTried.addEventListener('click', ()=>{
      ONLY_TRIED = !ONLY_TRIED;
      updateToggle(sbOnlyTried, ONLY_TRIED, 'Show all', 'Show only tried');
      apply(); resort();
      try { localStorage.setItem('pref:onlyTried', ONLY_TRIED ? '1' : '0'); } catch (_) {}
    });
    
    // Section toggle: click + keyboard + aria-expanded
    function setupSectionToggle(headerEl, sectionEl){
      if (!headerEl || !sectionEl) return;
      function setExpanded(){
        const expanded = !sectionEl.classList.contains('collapsed');
        headerEl.setAttribute('aria-expanded', expanded ? 'true' : 'false');
      }
      headerEl.addEventListener('click', () => {
        sectionEl.classList.toggle('collapsed');
        setExpanded();
      });
      headerEl.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar'){
          e.preventDefault();
          sectionEl.classList.toggle('collapsed');
          setExpanded();
        }
      });
      setExpanded();
    }
    setupSectionToggle(headerIn, secIn);
    setupSectionToggle(headerOut, secOut);

    // Carousel controls
    updateInButtons = setupCarousel(gridIn, prevIn, nextIn);
    updateOutButtons = setupCarousel(gridOut, prevOut, nextOut);

    // Load more pagination
    const loadMoreBtn = document.getElementById('loadMore');
    if (loadMoreBtn) {
      loadMoreBtn.style.display = '';
      loadMoreBtn.addEventListener('click', async ()=>{
        loadMoreBtn.disabled = true;
        try {
          const more = await fetchPage(60);
          if (!more.length) { loadMoreBtn.textContent = 'No more'; return; }
          const startLen = ITEMS.length;
          ITEMS = ITEMS.concat(more.map(x=>({ ...x, tried:false, purchased:false })));
          const frag = document.createDocumentFragment();
          for (let i = startLen; i < ITEMS.length; i++){
            const it = ITEMS[i];
            const a = document.createElement('a');
            a.className='card';
            a.href = it.url; a.target='_blank'; a.rel='noopener';
            a.dataset.id = it.id;
            a.dataset.roaster = it.roaster || '';
            a.dataset.roastername = it.roaster || '';
            a.dataset.title   = String(it.title ?? '').toLowerCase();
            a.dataset.price   = String(it.price ?? '').toLowerCase();
            a.dataset.notes   = String(it.notes ?? '').toLowerCase();
            a.dataset.country = String(it.country ?? '').toLowerCase();
            a.dataset.countryname = it.country || '';
            a.dataset.process = String(it.process ?? '').toLowerCase();
            a.dataset.profile = String(it.profile ?? '').toLowerCase();
            a.dataset.stock   = it.in_stock? 'in':'out';
            a.dataset.timelast = String(parseTime(it.last_seen));
            a.dataset.tried = it.tried ? '1' : '0';
            a.dataset.purchased = it.purchased ? '1' : '0';
            const roasterName = it.roaster || 'Unknown';
            if (!roasterColors.has(roasterName)) {
              const hash = [...roasterName].reduce((acc, char) => char.charCodeAt(0) + ((acc << 5) - acc), 0);
              const hue = hash % 360;
              roasterColors.set(roasterName, { solid:`hsl(${hue}, 85%, 70%)`, border:`hsl(${hue}, 70%, 55%)`, bg:`hsl(${hue}, 75%, 20%)` });
            }
            const colors = roasterColors.get(roasterName);
            a.style.setProperty('--roaster-solid', colors.solid);
            a.style.setProperty('--roaster-border', colors.border);
            a.style.setProperty('--roaster-bg', colors.bg);
            a.innerHTML = `<div class="card-content">
        <div class='row'><div class='chip roaster'>${esc(it.roaster||'')}</div><div class='chip ${it.in_stock?'ok':'no'}'>${it.in_stock?'In stock':'Sold out'}</div></div>
        <div class='title-line'>${esc(it.title||'')}</div>
        <div class='tags'>
          ${it.country ? `<span class='chip' title='Origin'>${esc(it.country)}</span>` : ''}
          ${it.process ? `<span class='chip' title='Process'>${esc(it.process)}</span>` : ''}
          ${it.profile ? `<span class='chip' title='Profile'>${esc(it.profile)}</span>` : ''}
          ${it.tried ? `<span class='chip ok' title='Tried' data-chip='tried'>Tried</span>` : ''}
          ${it.purchased ? `<span class='chip' title='Purchased' data-chip='purchased'>Purchased</span>` : ''}
        </div>
        <div class='muted'>${esc(it.price||'')}</div>
        <div class='row small'><div>First: ${esc(it.first_seen||'')}</div><div>Last: ${esc(it.last_seen||'')}</div></div>
      </div>`;
            const roasterEl = a.querySelector('.row .chip');
            if (roasterEl) roasterEl.title = (it.roaster||'');
            const titleEl = a.querySelector('.title-line');
            if (titleEl) titleEl.title = (it.title||'');
            a.addEventListener('click', (e)=>{ e.preventDefault(); openModal(it); });
            frag.appendChild(a);
            CARDS.push(a);
          }
          if (gridIn) gridIn.appendChild(frag.cloneNode(true));
          if (gridOut) gridOut.appendChild(frag);
          await loadUserSets();
        } finally {
          loadMoreBtn.disabled = false;
        }
      });
    }

    // Mouse effects for cards (only enable on hover-capable, fine pointer devices)
    const CAN_HOVER_FINE = window.matchMedia('(hover: hover) and (pointer: fine)').matches;
    if (CAN_HOVER_FINE) {
      [gridIn, gridOut].forEach(grid => {
        if (!grid) return;

        let lastHoveredCard = null;
        let rafId = 0;
        let lastClientX = 0;
        let lastClientY = 0;
        const activeBleed = new Set(); // cards with non-zero bleed last frame
        const MAX_RADIUS = 160; // px falloff for grid bleed (smaller to localize)
        const MAX_NEIGHBORS = 3; // cap number of cards that can bleed at once
        const MIN_VIS = 0.05; // minimum visible intensity to show bleed
        const EDGE_FALLOFF = 140; // px from edge to pool across (matches linear-gradient length)

        const clamp01 = v => v < 0 ? 0 : (v > 1 ? 1 : v);
        const easeOut = v => 1 - Math.pow(1 - v, 2); // simple easing for nicer falloff

        function processFrame() {
          rafId = 0;

          // Update grid-relative mouse position (for background hotspot alignment)
          const gridRect = grid.getBoundingClientRect();
          const gridX = lastClientX - gridRect.left;
          const gridY = lastClientY - gridRect.top;
          grid.style.setProperty('--grid-mouse-x', `${gridX}px`);
          grid.style.setProperty('--grid-mouse-y', `${gridY}px`);

          // Determine current hovered card if any
          const el = document.elementFromPoint(lastClientX, lastClientY);
          const currentCard = el && el.closest ? el.closest('.card') : null;

          // Reset previous hovered card tilt and edge pooling if moved off
          if (lastHoveredCard && lastHoveredCard !== currentCard) {
            lastHoveredCard.style.setProperty('--tilt-x', '0deg');
            lastHoveredCard.style.setProperty('--tilt-y', '0deg');
            lastHoveredCard.style.setProperty('--edge-left', '0');
            lastHoveredCard.style.setProperty('--edge-right', '0');
            lastHoveredCard.style.setProperty('--edge-top', '0');
            lastHoveredCard.style.setProperty('--edge-bottom', '0');
          }

          // Edge pooling + tilt on hovered card
          if (currentCard && grid.contains(currentCard)) {
            const r = currentCard.getBoundingClientRect();
            const cx = lastClientX - r.left;
            const cy = lastClientY - r.top;

            currentCard.style.setProperty('--card-mouse-x', `${cx}px`);
            currentCard.style.setProperty('--card-mouse-y', `${cy}px`);

            const tiltX = (r.height / 2 - cy) / (r.height / 2) * -8;
            const tiltY = (cx - r.width / 2) / (r.width / 2) * 8;
            currentCard.style.setProperty('--tilt-x', `${tiltX}deg`);
            currentCard.style.setProperty('--tilt-y', `${tiltY}deg`);

            // Pooling: stronger as pointer approaches that edge
            const l = clamp01(1 - cx / EDGE_FALLOFF);
            const rgt = clamp01(1 - (r.width - cx) / EDGE_FALLOFF);
            const t = clamp01(1 - cy / EDGE_FALLOFF);
            const btm = clamp01(1 - (r.height - cy) / EDGE_FALLOFF);
            currentCard.style.setProperty('--edge-left', String(easeOut(l)));
            currentCard.style.setProperty('--edge-right', String(easeOut(rgt)));
            currentCard.style.setProperty('--edge-top', String(easeOut(t)));
            currentCard.style.setProperty('--edge-bottom', String(easeOut(btm)));

            lastHoveredCard = currentCard;
          } else {
            lastHoveredCard = null;
          }

          // Bleed into nearest cards only: compute distance to card bounds and keep top-k
          const cardsInGrid = CARDS.filter(c => grid.contains(c));
          const nextActive = new Set();
          const scored = [];
          for (const c of cardsInGrid) {
            const r = c.getBoundingClientRect();
            const rect = c.getBoundingClientRect();
            c.style.setProperty('--mouse-x', `${lastClientX - rect.left}px`);
            c.style.setProperty('--mouse-y', `${lastClientY - rect.top}px`);

            // distance from point to the rectangle (0 if inside)
            const dx = lastClientX < r.left ? (r.left - lastClientX) : (lastClientX > r.right ? (lastClientX - r.right) : 0);
            const dy = lastClientY < r.top  ? (r.top  - lastClientY) : (lastClientY > r.bottom ? (lastClientY - r.bottom) : 0);
            const dist = Math.hypot(dx, dy);
            scored.push({ c, dist });
          }
          scored.sort((a,b)=>a.dist - b.dist);
          for (let i=0; i<scored.length; i++) {
            const { c, dist } = scored[i];
            if (i < MAX_NEIGHBORS && dist <= MAX_RADIUS) {
              let v = 1 - (dist / MAX_RADIUS);
              // Faster falloff
              v = clamp01(v);
              v = easeOut(v * v);
              if (v >= MIN_VIS) {
                c.style.setProperty('--bleed', String(v));
                c.setAttribute('data-bleed', '1');
                nextActive.add(c);
              }
            }
          }
          // Reset bleed on cards that are no longer active
          for (const prev of activeBleed) {
            if (!nextActive.has(prev)) {
              prev.style.setProperty('--bleed', '0');
              prev.removeAttribute('data-bleed');
            }
          }
          activeBleed.clear();
          nextActive.forEach(c => activeBleed.add(c));
        }

        grid.addEventListener('mousemove', e => {
          lastClientX = e.clientX;
          lastClientY = e.clientY;
          if (!rafId) rafId = requestAnimationFrame(processFrame);
        });

        grid.addEventListener('mouseleave', () => {
          if (rafId) { cancelAnimationFrame(rafId); rafId = 0; }
          // Reset tilt/edges on last hovered
          if (lastHoveredCard) {
            lastHoveredCard.style.setProperty('--tilt-x', '0deg');
            lastHoveredCard.style.setProperty('--tilt-y', '0deg');
            lastHoveredCard.style.setProperty('--edge-left', '0');
            lastHoveredCard.style.setProperty('--edge-right', '0');
            lastHoveredCard.style.setProperty('--edge-top', '0');
            lastHoveredCard.style.setProperty('--edge-bottom', '0');
            lastHoveredCard = null;
          }
          // Clear bleed from all active cards
          for (const c of activeBleed) { c.style.setProperty('--bleed', '0'); c.removeAttribute('data-bleed'); }
          activeBleed.clear();
        });
      });
    }
    apply();
    resort();

    // deep link(s)
    // params already defined above
    const idsParam = params.get('ids');
    const singleId = params.get('id');

    NEW_IDS = idsParam ? idsParam.split(',').filter(Boolean) : [];
    if (singleId && !NEW_IDS.includes(singleId)) {
      NEW_IDS.unshift(singleId);
    }

    // Show banner if multiple IDs
    if (NEW_IDS.length) {
      const found = NEW_IDS.map(id => byId(id)).filter(Boolean);
      if (found.length) {
        newCount.textContent = (found.length === 1)
          ? `1 new coffee`
          : `${found.length} new coffees`;
        newChips.innerHTML = '';
        for (const it of found) {
          const chip = document.createElement('button');
          chip.className = 'chip';
          chip.style.cursor = 'pointer';
          chip.title = it.title;
          chip.textContent = `${it.roaster} — ${it.title}`;
          chip.addEventListener('click', ()=> jumpToId(it.id));
          newChips.appendChild(chip);
        }
        newBanner.style.display = 'block';
      }
    }

    // Open first id if present, else just single id
    if (NEW_IDS.length) {
      jumpToId(NEW_IDS[0]);
    } else if (singleId) {
      const it = byId(singleId);
      if (it) {
        openModal(it);
        const el = CARDS.find(c=>c.dataset.id===singleId);
        if (el) el.scrollIntoView({behavior: PREFERS_REDUCED_MOTION ? 'auto' : 'smooth', block:'center'});
      }
    }

    // Auth lifecycle and menu controls
    function setMenuOpen(open){
      if (!authMenu || !avatarBtn) return;
      // Position menu aligned to avatar, with viewport clamping
      const btnRect = avatarBtn.getBoundingClientRect();
      const scrollY = window.scrollY || document.documentElement.scrollTop;
      const scrollX = window.scrollX || document.documentElement.scrollLeft;
      const margin = 8;
      const desiredTop = btnRect.bottom + margin + scrollY;
      let left = btnRect.left + scrollX - 24; // default arrow at ~24px
      // Clamp within viewport
      const menuWidth = Math.min(360, document.documentElement.clientWidth * 0.92);
      if (left + menuWidth > scrollX + document.documentElement.clientWidth - margin) {
        left = scrollX + document.documentElement.clientWidth - margin - menuWidth;
      }
      if (left < scrollX + margin) left = scrollX + margin;
      authMenu.style.position = 'absolute';
      authMenu.style.top = desiredTop + 'px';
      authMenu.style.left = left + 'px';
      // Arrow position relative to menu
      const arrowLeft = Math.max(16, Math.min(btnRect.left + scrollX - left + btnRect.width/2 - 8, menuWidth - 32));
      authMenu.style.setProperty('--arrow-left', arrowLeft + 'px');
      // Toggle
      authMenu.style.display = open ? 'block' : 'none';
      authMenu.setAttribute('aria-hidden', open ? 'false' : 'true');
      avatarBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
      if (menuBackdrop) menuBackdrop.style.display = open ? 'block' : 'none';
    }
    if (avatarBtn) avatarBtn.addEventListener('click', (e)=>{
      e.stopPropagation();
      const open = !authMenu || authMenu.getAttribute('aria-hidden') !== 'false';
      setMenuOpen(open);
    });
    if (authMenu) authMenu.addEventListener('click', (e)=>{ e.stopPropagation(); });
    if (menuBackdrop) menuBackdrop.addEventListener('click', (e)=>{ e.stopPropagation(); setMenuOpen(false); });
    window.addEventListener('click', (e)=>{
      if (!authMenu || authMenu.getAttribute('aria-hidden') !== 'false') return;
      const path = typeof e.composedPath === 'function' ? e.composedPath() : [];
      const clickedInsideMenu = authMenu.contains(e.target) || path.indexOf(authMenu) !== -1;
      const clickedAvatar = avatarBtn && (avatarBtn.contains(e.target) || path.indexOf(avatarBtn) !== -1);
      if (!clickedInsideMenu && !clickedAvatar) setMenuOpen(false);
    });
    if (menuGoogle) menuGoogle.addEventListener('click', async () => {
      try { await signInWithPopup(auth, provider); setMenuOpen(false);} catch (e){ console.error('Sign-in failed', e); alert('Sign-in failed.'); }
    });
    if (emailSignIn) emailSignIn.addEventListener('click', async ()=>{
      try {
        await signInWithEmailAndPassword(auth, (authEmail && authEmail.value)||'', (authPass && authPass.value)||'');
        if (typeof setMenuOpen === 'function') setMenuOpen(false);
      } catch (e) {
        console.error('Email sign-in failed', e);
        alert('Email sign-in failed.');
      }
    });
    if (emailRegister) emailRegister.addEventListener('click', async ()=>{
      try {
        await createUserWithEmailAndPassword(auth, (authEmail && authEmail.value)||'', (authPass && authPass.value)||'');
        alert('Registered. You are now signed in.');
        if (typeof setMenuOpen === 'function') setMenuOpen(false);
      } catch (e) {
        console.error('Register failed', e);
        alert('Register failed.');
      }
    });
    if (menuSignOut) menuSignOut.addEventListener('click', async () => { try { await fbSignOut(auth); setMenuOpen(false);} catch (e) { console.error('Sign-out failed', e); } });
    onAuthStateChanged(auth, async (user) => {
      CURRENT_USER = user || null;
      updateAuthUI();
      await loadUserSets();
    });
  })();
