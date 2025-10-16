// Utilities and cross-file globals

// Cleanup handlers registry
window.cleanupHandlers = [];
window.addCleanupHandler = function addCleanupHandler(handler) {
  window.cleanupHandlers.push(handler);
};
window.cleanupAll = function cleanupAll() {
  window.cleanupHandlers.forEach((handler) => {
    try { handler(); } catch (e) { console.error('Cleanup error:', e); }
  });
  window.cleanupHandlers.length = 0;
};

// Debounce helper
window.debounce = function debounce(func, wait) {
  let timeout;
  return function executedFunction(...args) {
    const later = () => {
      clearTimeout(timeout);
      func(...args);
    };
    clearTimeout(timeout);
    timeout = setTimeout(later, wait);
  };
};

// Clipboard detection and paste helper
window.clipboardAvailable = false;
window.checkClipboardAvailability = function checkClipboardAvailability() {
  window.clipboardAvailable = !!(navigator.clipboard && navigator.clipboard.readText && window.isSecureContext);
  return window.clipboardAvailable;
};

window.pasteToInput = async function pasteToInput(inputId) {
  const input = document.getElementById(inputId);
  if (!input || !window.clipboardAvailable) return;
  try {
    const text = await navigator.clipboard.readText();
    input.value = text;
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.classList.add('bg-green-50');
    setTimeout(() => input.classList.remove('bg-green-50'), 300);
  } catch (err) {
    console.error('Failed to read clipboard:', err);
    input.classList.add('bg-red-50');
    setTimeout(() => input.classList.remove('bg-red-50'), 300);
  }
};

// HTML escaping for safe insertion
window.escapeHtml = function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
};

