// Output rendering, clickable words, timestamps, copy helpers
(function() {
  // Public API
  window.startCommandOutput = startCommandOutput;
  window.appendToCommandOutput = appendToCommandOutput;
  window.appendOutput = appendOutput;
  window.showError = showError;
  window.copyOutput = copyOutput;

  // State
  let currentCommandBox = null;
  window.currentActiveCommand = null;

  // Word click/tooltip state
  let currentTooltip = null;
  let currentTooltipElement = null;
  let tooltipHideTimer = null;
  let shiftSelectionState = { isActive: false, startElement: null, allClickableElements: [] };

  function startCommandOutput(command) {
    const output = document.getElementById('output');
    const commandBox = document.createElement('div');
    commandBox.className = 'command-output-box';

    const header = document.createElement('div');
    header.className = 'command-header';
    header.innerHTML = `<span class="word-clickable" data-word="${escapeHtml(command)}">$ ${escapeHtml(command)}</span>`;

    const content = document.createElement('div');
    content.className = 'command-content';

    commandBox.appendChild(header);
    commandBox.appendChild(content);
    output.appendChild(commandBox);
    currentCommandBox = content;
    scrollOutputToBottom();
  }

  function appendToCommandOutput(text, type = 'normal', isRawHtml = false) {
    if (!currentCommandBox) {
      appendOutput(text);
      return;
    }
    const processedText = isRawHtml ? text : makeWordsClickable(text);
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = processedText;
    if (type === 'success') tempDiv.classList.add('success-output');
    else if (type === 'error') tempDiv.classList.add('error-output');
    else if (type === 'info') tempDiv.classList.add('info-output');
    while (tempDiv.firstChild) currentCommandBox.appendChild(tempDiv.firstChild);
    scrollOutputToBottom();
  }

  function appendOutput(text) {
    const output = document.getElementById('output');
    const processedText = makeWordsClickable(text);
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = processedText;
    while (tempDiv.firstChild) output.appendChild(tempDiv.firstChild);
    scrollOutputToBottom();
  }

  function showError(message) {
    startCommandOutput('error');
    appendToCommandOutput(`❌ ${message}\n`, 'error');
  }

  async function copyOutput() {
    const output = document.getElementById('output');
    const text = output.textContent || output.innerText;
    try {
      await navigator.clipboard.writeText(text);
      const copyButton = document.querySelector('button[onclick="copyOutput()"]');
      if (!copyButton) return;
      const original = copyButton.innerHTML;
      copyButton.innerHTML = '<i class="fas fa-check mr-1"></i>Copied!';
      copyButton.classList.remove('bg-blue-500', 'hover:bg-blue-600');
      copyButton.classList.add('bg-green-500', 'hover:bg-green-600');
      setTimeout(() => {
        copyButton.innerHTML = original;
        copyButton.classList.remove('bg-green-500', 'hover:bg-green-600');
        copyButton.classList.add('bg-blue-500', 'hover:bg-blue-600');
      }, 2000);
    } catch (error) {
      console.error('Failed to copy to clipboard:', error);
      showError('Failed to copy to clipboard');
    }
  }

  function scrollOutputToBottom() {
    const output = document.getElementById('output');
    requestAnimationFrame(() => { output.scrollTop = output.scrollHeight; });
  }

  function makeWordsClickable(text) {
    const lines = text.split('\n');
    return lines.map((line) => {
      const parts = line.split(/(\s+|:|\(|\)|\[|\]|=|,|{|}|")/);
      return parts
        .map((part) => {
          if (/^\s+$/.test(part)) return part;
          if (part.length > 0) {
            const isTimestamp = isValidTimestamp(part);
            const extraClass = isTimestamp ? ' timestamp-value' : '';
            return `<span class="word-clickable${extraClass}" data-word="${escapeHtml(part)}">${escapeHtml(part)}</span>`;
          }
          return part;
        })
        .join('');
    }).join('\n');
  }

  function isValidTimestamp(str) {
    if (!/^\d+$/.test(str)) return false;
    if (str.length >= 18 && str.length <= 19) {
      const result = parseMilvusTSFromString(str);
      if (result) {
        const { physicalTime } = result;
        const minDate = new Date('1900-01-01').getTime();
        const maxDate = new Date('2100-01-01').getTime();
        return physicalTime >= minDate && physicalTime <= maxDate;
      }
    }
    if (str.length < 10 || str.length > 13) return false;
    const num = parseInt(str);
    if (isNaN(num)) return false;
    const timestamp = str.length === 10 ? num * 1000 : num;
    const min = new Date('1900-01-01').getTime();
    const max = new Date('2100-01-01').getTime();
    return timestamp >= min && timestamp <= max;
  }

  function parseMilvusTSFromString(tsStr) {
    try {
      const ts = BigInt(tsStr);
      const logicalBits = 18n;
      const logicalBitsMask = (1n << logicalBits) - 1n;
      const logical = Number(ts & logicalBitsMask);
      const physical = Number(ts >> logicalBits);
      const physicalTime = physical;
      return { physicalTime, logical, ts: tsStr };
    } catch (e) {
      return null;
    }
  }

  // Word interactions and timestamp tooltip
  function initializeWordClickHandlers() {
    const output = document.getElementById('output');
    if (output) {
      output.addEventListener('click', (event) => {
        if (event.target.classList.contains('word-clickable')) {
          handleWordClick(event);
        }
      });
      output.addEventListener('mouseover', (event) => {
        if (event.target.classList.contains('word-clickable')) {
          if (shiftSelectionState.isActive) handleWordHover(event);
          if (event.target.classList.contains('timestamp-value')) showTimestampTooltip(event.target);
        }
      });
      output.addEventListener('mouseout', (event) => {
        if (event.target.classList.contains('word-clickable')) {
          if (shiftSelectionState.isActive) clearSelectionPreview();
          if (event.target.classList.contains('timestamp-value')) hideTimestampTooltip();
        }
      });
    }
    document.addEventListener('keyup', (event) => {
      if (event.key === 'Shift' && shiftSelectionState.isActive) endShiftSelection();
    });
  }
  window.initializeWordClickHandlers = initializeWordClickHandlers;

  function handleWordClick(event) {
    const element = event.target;
    const word = element.getAttribute('data-word');
    if (!word) return;
    if (event.shiftKey) {
      event.preventDefault();
      event.stopPropagation();
      handleShiftClick(element, word);
    } else {
      if (shiftSelectionState.isActive) endShiftSelection();
      copyWord(word);
    }
  }

  function handleShiftClick(element) {
    if (!shiftSelectionState.isActive) {
      startShiftSelection(element);
    } else {
      if (element === shiftSelectionState.startElement) {
        const word = element.getAttribute('data-word');
        copyWord(word);
        endShiftSelection();
      } else {
        completeRangeSelection(element);
      }
    }
  }

  function startShiftSelection(startElement) {
    shiftSelectionState.isActive = true;
    shiftSelectionState.startElement = startElement;
    updateClickableElementsList();
    startElement.classList.add('selection-start');
    document.body.style.userSelect = 'none';
    document.body.style.webkitUserSelect = 'none';
    document.body.style.mozUserSelect = 'none';
    document.body.style.msUserSelect = 'none';
  }

  function updateClickableElementsList() {
    const output = document.getElementById('output');
    shiftSelectionState.allClickableElements = Array.from(output.querySelectorAll('.word-clickable'));
  }

  function handleWordHover(event) {
    if (!shiftSelectionState.isActive) return;
    const hoveredElement = event.target;
    clearSelectionPreview();
    const range = getElementRange(shiftSelectionState.startElement, hoveredElement);
    if (range) {
      range.forEach((el) => { if (el !== shiftSelectionState.startElement) el.classList.add('selection-preview'); });
    }
  }

  function clearSelectionPreview() {
    shiftSelectionState.allClickableElements.forEach((el) => el.classList.remove('selection-preview'));
  }

  function completeRangeSelection(endElement) {
    const range = getElementRange(shiftSelectionState.startElement, endElement);
    if (range && range.length > 0) {
      const selectedText = getOriginalTextFromRange(range);
      copyWord(selectedText);
      clearSelectionPreview();
      range.forEach((el) => el.classList.add('selection-range'));
      setTimeout(() => range.forEach((el) => el.classList.remove('selection-range')), 1000);
    }
    endShiftSelection();
  }

  function getElementRange(startElement, endElement) {
    const startIndex = shiftSelectionState.allClickableElements.indexOf(startElement);
    const endIndex = shiftSelectionState.allClickableElements.indexOf(endElement);
    if (startIndex === -1 || endIndex === -1) return null;
    const minIndex = Math.min(startIndex, endIndex);
    const maxIndex = Math.max(startIndex, endIndex);
    return shiftSelectionState.allClickableElements.slice(minIndex, maxIndex + 1);
  }

  function getOriginalTextFromRange(elements) {
    if (!elements || elements.length === 0) return '';
    const firstElement = elements[0];
    const lastElement = elements[elements.length - 1];
    let commonParent = firstElement.parentNode;
    while (commonParent && !commonParent.contains(lastElement)) commonParent = commonParent.parentNode;
    if (!commonParent) return elements.map((el) => el.getAttribute('data-word')).join('');
    const range = document.createRange();
    range.setStartBefore(firstElement);
    range.setEndAfter(lastElement);
    return range.toString();
  }

  function endShiftSelection() {
    if (!shiftSelectionState.isActive) return;
    shiftSelectionState.allClickableElements.forEach((el) => el.classList.remove('selection-start', 'selection-preview', 'selection-range'));
    document.body.style.userSelect = '';
    document.body.style.webkitUserSelect = '';
    document.body.style.mozUserSelect = '';
    document.body.style.msUserSelect = '';
    shiftSelectionState.isActive = false;
    shiftSelectionState.startElement = null;
    shiftSelectionState.allClickableElements = [];
  }

  async function copyWord(word) {
    let success = false;
    try {
      if (navigator.clipboard && navigator.clipboard.writeText && window.isSecureContext) {
        await navigator.clipboard.writeText(word);
        success = true;
      }
    } catch (e) {
      console.log('Clipboard API failed, trying fallback:', e.message);
    }
    if (!success) success = fallbackCopyToClipboard(word);
    if (success) showCopyNotification(word); else showCopyError();
  }

  function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    let success = false;
    try { success = document.execCommand('copy'); } catch (err) { console.error('Fallback copy failed:', err); }
    document.body.removeChild(textArea);
    return success;
  }

  function showCopyError() {
    const notification = document.createElement('div');
    notification.className = 'copy-notification bg-red-500';
    notification.innerHTML = `
      <i class="fas fa-exclamation-triangle mr-2"></i>
      <span>Copy failed - Select text and use Ctrl+C</span>
    `;
    document.body.appendChild(notification);
    setTimeout(() => {
      notification.classList.add('hide');
      setTimeout(() => notification.parentNode && notification.parentNode.removeChild(notification), 300);
    }, 2000);
  }

  function showCopyNotification(word) {
    const existing = document.querySelector('.copy-notification');
    if (existing) existing.remove();
    const notification = document.createElement('div');
    notification.className = 'copy-notification';
    notification.innerHTML = `<i class=\"fas fa-check mr-2\"></i>Copied: \"${word}\"`;
    document.body.appendChild(notification);
    setTimeout(() => {
      notification.classList.add('hide');
      setTimeout(() => notification.parentNode && notification.parentNode.removeChild(notification), 300);
    }, 1500);
  }

  function showTimestampTooltip(element) {
    if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
    if (currentTooltipElement === element && currentTooltip && currentTooltip.parentNode) return;
    const timestampStr = element.getAttribute('data-word');
    let timestampMs;
    let milvusInfo = '';
    if (timestampStr.length >= 18 && timestampStr.length <= 19) {
      const result = parseMilvusTSFromString(timestampStr);
      if (result) { timestampMs = result.physicalTime; milvusInfo = `<div class="milvus-info">Milvus TS: ${timestampStr} (Logical: ${result.logical})</div>`; }
      else {
        const timestamp = parseInt(timestampStr);
        if (isNaN(timestamp)) return;
        timestampMs = timestamp;
      }
    } else {
      const timestamp = parseInt(timestampStr);
      if (isNaN(timestamp)) return;
      timestampMs = timestampStr.length === 10 ? timestamp * 1000 : timestamp;
    }
    const date = new Date(timestampMs);
    const utcString = date.toUTCString();
    const localString = date.toLocaleString(undefined, {
      year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', timeZoneName: 'short'
    });
    const now = new Date();
    const diffMs = Math.abs(now - date);
    const diffDays = diffMs / (1000 * 60 * 60 * 24);
    let relativeTimeHtml = '';
    if (diffDays <= 30) relativeTimeHtml = `<div class="relative-time">Relative: ${formatRelativeTime(date, now)}</div>`;
    hideTimestampTooltip(true);
    const tooltip = document.createElement('div');
    tooltip.className = 'timestamp-tooltip';
    tooltip.innerHTML = `
      ${milvusInfo}
      <div class="utc-time">UTC: ${utcString}</div>
      <div class="local-time">Local: ${localString}</div>
      ${relativeTimeHtml}
    `;
    document.body.appendChild(tooltip);
    tooltip.addEventListener('mouseenter', () => { if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; } });
    tooltip.addEventListener('mouseleave', () => { hideTimestampTooltip(); });
    const rect = element.getBoundingClientRect();
    const tooltipHeight = 120;
    tooltip.style.position = 'fixed';
    tooltip.style.left = `${rect.left + rect.width / 2}px`;
    tooltip.style.transform = 'translateX(-50%)';
    if (rect.top - tooltipHeight - 8 < 0) tooltip.style.top = `${rect.bottom + 8}px`; else { tooltip.style.bottom = `${window.innerHeight - rect.top + 8}px`; tooltip.style.top = 'auto'; }
    currentTooltip = tooltip;
    currentTooltipElement = element;
    setTimeout(() => { if (tooltip.parentNode) tooltip.classList.add('show'); }, 10);
  }

  function formatRelativeTime(date, now) {
    const diffMs = now - date;
    const isPast = diffMs > 0;
    const abs = Math.abs(diffMs);
    const seconds = Math.floor(abs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    const units = [];
    if (days > 0) {
      units.push(`${days} day${days !== 1 ? 's' : ''}`);
      const rh = hours % 24;
      if (rh > 0) units.push(`${rh} hour${rh !== 1 ? 's' : ''}`); else {
        const rm = minutes % 60; if (rm > 0) units.push(`${rm} minute${rm !== 1 ? 's' : ''}`);
      }
    } else if (hours > 0) {
      units.push(`${hours} hour${hours !== 1 ? 's' : ''}`);
      const rm = minutes % 60; if (rm > 0) units.push(`${rm} minute${rm !== 1 ? 's' : ''}`); else { const rs = seconds % 60; units.push(`${rs} second${rs !== 1 ? 's' : ''}`); }
    } else if (minutes > 0) {
      units.push(`${minutes} minute${minutes !== 1 ? 's' : ''}`);
      const rs = seconds % 60; units.push(`${rs} second${rs !== 1 ? 's' : ''}`);
    } else {
      units.push(`${seconds} second${seconds !== 1 ? 's' : ''}`);
    }
    const s = units.slice(0, 2).join(' ');
    if (abs < 1000) return 'just now';
    return isPast ? `${s} ago` : `in ${s}`;
  }

  function hideTimestampTooltip(immediate = false) {
    if (tooltipHideTimer) { clearTimeout(tooltipHideTimer); tooltipHideTimer = null; }
    if (immediate) {
      if (currentTooltip && currentTooltip.parentNode) currentTooltip.parentNode.removeChild(currentTooltip);
      currentTooltip = null; currentTooltipElement = null; return;
    }
    tooltipHideTimer = setTimeout(() => {
      if (currentTooltip && currentTooltip.parentNode) {
        currentTooltip.classList.remove('show');
        setTimeout(() => {
          if (currentTooltip && currentTooltip.parentNode) currentTooltip.parentNode.removeChild(currentTooltip);
          currentTooltip = null; currentTooltipElement = null;
        }, 200);
      }
      tooltipHideTimer = null;
    }, 100);
  }
})();

