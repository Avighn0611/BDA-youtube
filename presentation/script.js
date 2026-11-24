(() => {
  const BASE_WIDTH = 1920;
  const BASE_HEIGHT = 1080;

  const slides = Array.from(document.querySelectorAll('.slide'));
  const stage = document.getElementById('slideStage');
  const counterEl = document.getElementById('navCounter');
  const hud = document.getElementById('navHud');
  const overviewModal = document.getElementById('overviewModal');
  const overviewGrid = document.getElementById('overviewGrid');

  let currentIndex = 0;
  let hudTimer = null;

  function scaleStage() {
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const scale = Math.min(vw / BASE_WIDTH, vh / BASE_HEIGHT);
    stage.style.transform = `scale(${scale})`;
  }

  function updateFooters() {
    const total = slides.length;
    slides.forEach((slide, idx) => {
      const numEl = slide.querySelector('.slide-num');
      const barEl = slide.querySelector('.progress-fill');
      if (numEl) {
        numEl.textContent = `${String(idx + 1).padStart(2, '0')} / ${String(total).padStart(2, '0')}`;
      }
      if (barEl) {
        barEl.style.width = `${((idx + 1) / total) * 100}%`;
      }
    });
  }

  function buildOverview() {
    overviewGrid.innerHTML = '';
    slides.forEach((slide, idx) => {
      const title = slide.dataset.title || `Slide ${idx + 1}`;
      const card = document.createElement('div');
      card.className = 'overview-card' + (idx === currentIndex ? ' current' : '');
      card.innerHTML = `
        <div class="overview-num">SLIDE ${String(idx + 1).padStart(2, '0')}</div>
        <div class="overview-title">${title}</div>
      `;
      card.addEventListener('click', () => {
        goToSlide(idx);
        toggleOverview(false);
      });
      overviewGrid.appendChild(card);
    });
  }

  function goToSlide(index, updateHash = true) {
    const bounded = Math.max(0, Math.min(slides.length - 1, index));
    slides.forEach((s, i) => s.classList.toggle('active', i === bounded));
    currentIndex = bounded;
    if (counterEl) {
      counterEl.textContent = `${String(currentIndex + 1).padStart(2, '0')} / ${String(slides.length).padStart(2, '0')}`;
    }
    if (updateHash) {
      history.replaceState(null, '', `#slide-${currentIndex + 1}`);
    }
    const cards = overviewGrid.querySelectorAll('.overview-card');
    cards.forEach((c, i) => c.classList.toggle('current', i === currentIndex));
  }

  function nextSlide() {
    goToSlide(currentIndex + 1);
  }

  function prevSlide() {
    goToSlide(currentIndex - 1);
  }

  function toggleFullscreen() {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen?.().catch(() => {});
    } else {
      document.exitFullscreen?.().catch(() => {});
    }
  }

  function toggleOverview(force) {
    const open = typeof force === 'boolean' ? force : !overviewModal.classList.contains('open');
    overviewModal.classList.toggle('open', open);
  }

  function showHudTemporarily() {
    hud.classList.add('visible');
    clearTimeout(hudTimer);
    hudTimer = setTimeout(() => hud.classList.remove('visible'), 2500);
  }

  function syncFromHash() {
    const match = window.location.hash.match(/^#slide-(\d+)$/);
    if (match) {
      const target = parseInt(match[1], 10) - 1;
      if (!Number.isNaN(target)) {
        goToSlide(target, false);
        return;
      }
    }
    goToSlide(0, false);
  }

  // Button listeners
  document.getElementById('btnPrev')?.addEventListener('click', prevSlide);
  document.getElementById('btnNext')?.addEventListener('click', nextSlide);
  document.getElementById('btnFullscreen')?.addEventListener('click', toggleFullscreen);
  document.getElementById('btnOverview')?.addEventListener('click', () => toggleOverview());
  document.getElementById('btnCloseOverview')?.addEventListener('click', () => toggleOverview(false));
  document.getElementById('btnPrint')?.addEventListener('click', () => window.print());

  // Keyboard navigation
  document.addEventListener('keydown', (e) => {
    if (overviewModal.classList.contains('open')) {
      if (e.key === 'Escape' || e.key.toLowerCase() === 'g' || e.key.toLowerCase() === 'o') {
        toggleOverview(false);
      }
      return;
    }

    switch (e.key) {
      case 'ArrowRight':
      case 'ArrowDown':
      case 'PageDown':
      case ' ':
        e.preventDefault();
        nextSlide();
        break;
      case 'ArrowLeft':
      case 'ArrowUp':
      case 'PageUp':
        e.preventDefault();
        prevSlide();
        break;
      case 'Home':
        e.preventDefault();
        goToSlide(0);
        break;
      case 'End':
        e.preventDefault();
        goToSlide(slides.length - 1);
        break;
      case 'f':
      case 'F':
        toggleFullscreen();
        break;
      case 'g':
      case 'G':
      case 'o':
      case 'O':
        toggleOverview(true);
        break;
      default:
        break;
    }
  });

  // Touch swipe support
  let touchStartX = 0;
  document.addEventListener('touchstart', (e) => {
    touchStartX = e.changedTouches[0].screenX;
  }, { passive: true });
  document.addEventListener('touchend', (e) => {
    const dx = e.changedTouches[0].screenX - touchStartX;
    if (Math.abs(dx) > 50) {
      if (dx < 0) nextSlide();
      else prevSlide();
    }
  }, { passive: true });

  window.addEventListener('resize', scaleStage);
  window.addEventListener('hashchange', syncFromHash);
  window.addEventListener('mousemove', showHudTemporarily, { passive: true });

  // Initialize
  updateFooters();
  buildOverview();
  scaleStage();
  syncFromHash();
})();
