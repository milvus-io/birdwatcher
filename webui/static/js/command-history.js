// Command history
(function() {
  const HISTORY_KEY = 'birdwatcher_command_history';
  const MAX_HISTORY = 100;

  window.commandHistory = [];

  window.loadCommandHistory = function loadCommandHistory() {
    try {
      const stored = localStorage.getItem(HISTORY_KEY);
      if (stored) window.commandHistory = JSON.parse(stored);
    } catch (e) {
      console.error('Failed to load command history:', e);
      window.commandHistory = [];
    }
  };

  window.saveToHistory = function saveToHistory(command) {
    if (!command || command.trim() === '') return;
    window.commandHistory = window.commandHistory.filter((cmd) => cmd !== command);
    window.commandHistory.unshift(command);
    if (window.commandHistory.length > MAX_HISTORY) {
      window.commandHistory = window.commandHistory.slice(0, MAX_HISTORY);
    }
    try {
      localStorage.setItem(HISTORY_KEY, JSON.stringify(window.commandHistory));
    } catch (e) {
      console.error('Failed to save command history:', e);
    }
  };

  window.getHistorySuggestions = function getHistorySuggestions(input) {
    if (!input) return window.commandHistory.slice(0, 10);
    const lower = input.toLowerCase();
    return window.commandHistory.filter((cmd) => cmd.toLowerCase().includes(lower)).slice(0, 20);
  };
})();

