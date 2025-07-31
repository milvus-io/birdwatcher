// Global state
let connectionState = {
    connected: false,
    etcdAddr: '',
    rootPath: ''
};

// Global variables for commands
let allCommandsData = {}; // Store all commands for searching
let categoryStates = {
    show: false,
    management: false,
    analysis: false
};

// Root path management state
let rootPathState = {
    userValue: 'by-dev', // Store user's custom value
    isFirstAutoUncheck: true // Track if it's the first time unchecking auto
};

// Check clipboard availability
let clipboardAvailable = false;
function checkClipboardAvailability() {
    clipboardAvailable = !!(navigator.clipboard && navigator.clipboard.readText && window.isSecureContext);
    return clipboardAvailable;
}

let currentActiveCommand = null;

// Global cleanup handlers
const cleanupHandlers = [];

// Add cleanup handler
function addCleanupHandler(handler) {
    cleanupHandlers.push(handler);
}

// Cleanup all handlers
function cleanupAll() {
    cleanupHandlers.forEach(handler => {
        try {
            handler();
        } catch (e) {
            console.error('Cleanup error:', e);
        }
    });
    cleanupHandlers.length = 0;
}

// Debounce function to limit API calls
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Paste clipboard content to input
async function pasteToInput(inputId) {
    const input = document.getElementById(inputId);
    if (!input || !clipboardAvailable) return;
    
    try {
        const text = await navigator.clipboard.readText();
        input.value = text;
        // Trigger input event for any listeners
        input.dispatchEvent(new Event('input', { bubbles: true }));
        // Flash the input to show paste happened
        input.classList.add('bg-green-50');
        setTimeout(() => {
            input.classList.remove('bg-green-50');
        }, 300);
    } catch (err) {
        console.error('Failed to read clipboard:', err);
        // Simply flash red on error
        input.classList.add('bg-red-50');
        setTimeout(() => {
            input.classList.remove('bg-red-50');
        }, 300);
    }
}


// Initialize the app
document.addEventListener('DOMContentLoaded', function() {
    // Check clipboard availability
    checkClipboardAvailability();
    
    // Hide paste buttons if clipboard not available
    if (!clipboardAvailable) {
        document.querySelectorAll('button[onclick^="pasteToInput"]').forEach(btn => {
            btn.style.display = 'none';
        });
        
        // Also hide custom command paste button
        const customPasteBtn = document.getElementById('custom-command-paste');
        if (customPasteBtn) {
            customPasteBtn.style.display = 'none';
        }
        
        // Show clipboard notice on connect screen
        const notice = document.getElementById('clipboard-notice');
        if (notice) {
            notice.classList.remove('hidden');
        }
    }
    
    // Check if already connected
    checkConnectionStatus();
    
    // Add Enter key handlers for connect screen
    setupConnectScreenHandlers();
    
    // Initialize resizer functionality
    initializeResizer();
    
    // Initialize word click handlers
    initializeWordClickHandlers();
    
    // Initialize autocomplete
    initializeAutocomplete();

    // Add beforeunload event listener to disconnect session
    const beforeUnloadHandler = function() {
        if (connectionState.connected) {
            // Use navigator.sendBeacon as it's reliable for unload events
            navigator.sendBeacon('/api/disconnect', new Blob());
        }
    };
    window.addEventListener('beforeunload', beforeUnloadHandler);
    addCleanupHandler(() => window.removeEventListener('beforeunload', beforeUnloadHandler));
});

// Setup Enter key handlers for connect screen
function setupConnectScreenHandlers() {
    const hostInput = document.getElementById('connect-etcd-host');
    const portInput = document.getElementById('connect-etcd-port');
    const rootPathInput = document.getElementById('connect-root-path');
    
    const enterHandler = function(e) {
        if (e.key === 'Enter') {
            connectToEtcd();
        }
    };
    
    if (hostInput) {
        hostInput.addEventListener('keypress', enterHandler);
    }
    
    if (portInput) {
        portInput.addEventListener('keypress', enterHandler);
    }
    
    if (rootPathInput) {
        rootPathInput.addEventListener('keypress', enterHandler);
    }
}

// Check connection status on load
async function checkConnectionStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        if (status.connected) {
            // Already connected, show main screen
            showMainScreen();
            loadCommands();
        } else {
            // Not connected, show connect screen
            showConnectScreen();
        }
    } catch (error) {
        console.error('Failed to check status:', error);
        showConnectScreen();
    }
}

// Show connect screen
function showConnectScreen() {
    document.getElementById('connect-screen').classList.add('active');
    document.getElementById('main-screen').classList.remove('active');
}

// Show main screen
function showMainScreen() {
    document.getElementById('connect-screen').classList.remove('active');
    document.getElementById('main-screen').classList.add('active');
    
    // Initialize resizer after showing main screen
    setTimeout(() => {
        initializeResizer();
        initializeAutocomplete(); // Re-initialize autocomplete on main screen
    }, 100);
}

// Toggle root path auto mode
function toggleRootPathAuto() {
    const autoCheckbox = document.getElementById('root-path-auto');
    const rootPathInput = document.getElementById('connect-root-path');
    const rootPathPasteBtn = document.getElementById('root-path-paste');
    
    if (autoCheckbox.checked) {
        // Save the current value if it's not --auto
        if (rootPathInput.value !== '--auto') {
            rootPathState.userValue = rootPathInput.value;
        }
        
        // Show --auto when auto is enabled
        rootPathInput.value = '--auto';
        rootPathInput.disabled = true;
        rootPathPasteBtn.disabled = true;
    } else {
        // Restore user value when auto is disabled
        if (rootPathState.isFirstAutoUncheck) {
            rootPathInput.value = 'by-dev';
            rootPathState.isFirstAutoUncheck = false;
        } else {
            rootPathInput.value = rootPathState.userValue;
        }
        
        rootPathInput.disabled = false;
        rootPathPasteBtn.disabled = false;
        rootPathInput.focus();
        
        // Add listener to save user input
        rootPathInput.oninput = function() {
            rootPathState.userValue = this.value;
        };
    }
}

// Connect to etcd from connect screen
async function connectToEtcd() {
    const etcdHost = document.getElementById('connect-etcd-host').value.trim();
    const etcdPort = document.getElementById('connect-etcd-port').value.trim();
    const rootPathInput = document.getElementById('connect-root-path');
    const autoCheckbox = document.getElementById('root-path-auto');
    
    if (!etcdHost || !etcdPort) {
        showConnectError('Please enter host and port');
        return;
    }
    
    // Check if auto mode is enabled
    const isAutoMode = autoCheckbox.checked;
    const rootPath = isAutoMode ? '' : rootPathInput.value.trim();
    
    if (!isAutoMode && !rootPath) {
        showConnectError('Please enter root path or enable Auto mode');
        return;
    }
    
    // Validate port number
    const portNum = parseInt(etcdPort);
    if (isNaN(portNum) || portNum < 1 || portNum > 65535) {
        showConnectError('Please enter a valid port number (1-65535)');
        return;
    }
    
    const etcdAddr = `${etcdHost}:${etcdPort}`;
    
    // Show loading state on connect button
    const connectBtn = document.getElementById('connect-button');
    const connectIcon = document.getElementById('connect-icon');
    const connectText = document.getElementById('connect-text');
    const connectSpinner = document.getElementById('connect-spinner');
    
    connectBtn.disabled = true;
    connectIcon.classList.add('hidden');
    connectSpinner.classList.remove('hidden');
    connectSpinner.classList.add('loading');
    connectText.textContent = 'Connecting...';
    
    hideConnectError();
    
    try {
        const response = await fetch('/api/connect', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                etcd_addr: etcdAddr,
                root_path: isAutoMode ? '__auto__' : rootPath
            })
        });
        
        const result = await response.json();
        
        if (result.connected) {
            connectionState = {
                connected: true,
                etcdAddr: etcdAddr,
                rootPath: isAutoMode ? 'auto' : rootPath
            };
            
            // Update connected info
            const displayInfo = isAutoMode ? `${etcdAddr} (auto mode)` : `${etcdAddr} (${rootPath})`;
            document.getElementById('connected-info').textContent = displayInfo;
            
            // Switch to main screen
            showMainScreen();
            loadCommands();
            
            // Create a connection success box
            startCommandOutput('connect');
            const successMsg = isAutoMode ? 
                `✅ Successfully connected to etcd at ${etcdAddr} using auto mode\n` :
                `✅ Successfully connected to etcd at ${etcdAddr} with root path ${rootPath}\n`;
            appendToCommandOutput(successMsg, 'success');
        } else {
            showConnectError(result.error || 'Connection failed');
        }
        
    } catch (error) {
        showConnectError('Failed to connect: ' + error.message);
    } finally {
        // Reset connect button state
        connectBtn.disabled = false;
        connectIcon.classList.remove('hidden');
        connectSpinner.classList.add('hidden');
        connectSpinner.classList.remove('loading');
        connectText.textContent = 'Connect to Birdwatcher';
    }
}

// Disconnect and return to connect screen
async function disconnect() {
    try {
        await fetch('/api/disconnect', { method: 'POST' });
    } catch (error) {
        console.error('Failed to notify server on disconnect:', error);
    }
    
    connectionState = {
        connected: false,
        etcdAddr: '',
        rootPath: ''
    };
    
    showConnectScreen();
    clearOutput();
    
    // Create a disconnect info box
    startCommandOutput('disconnect');
    appendToCommandOutput('Disconnected from etcd.\n', 'info');
}

// Show/hide connect error
function showConnectError(message) {
    const errorDiv = document.getElementById('connect-error');
    const errorMessage = document.getElementById('connect-error-message');
    errorMessage.textContent = message;
    errorDiv.classList.remove('hidden');
}

function hideConnectError() {
    document.getElementById('connect-error').classList.add('hidden');
}

// Load available commands from the API
async function loadCommands() {
    try {
        const response = await fetch('/api/commands');
        const commands = await response.json();
        
        // Store commands data for searching
        allCommandsData = commands;
        
        populateCommands('show-commands', commands.show, 'show');
        populateCommands('management-commands', commands.management, 'management');
        populateCommands('analysis-commands', commands.analysis, 'analysis');
        
        // Set initial collapsed state for all categories
        Object.keys(categoryStates).forEach(category => {
            const toggle = document.getElementById(`${category}-toggle`);
            const container = document.getElementById(`${category}-commands`);
            if (toggle && container) {
                if (!categoryStates[category]) {
                    container.style.display = 'none';
                    toggle.classList.add('collapsed');
                }
            }
        });
        
    } catch (error) {
        console.error('Failed to load commands:', error);
    }
}

// Populate command buttons with improved styling and expandable interfaces
function populateCommands(containerId, commands, category) {
    const container = document.getElementById(containerId);
    container.innerHTML = '';
    
    commands.forEach(cmd => {
        const commandDiv = document.createElement('div');
        commandDiv.className = 'command-item mb-2';
        commandDiv.setAttribute('data-command-name', cmd.name);
        commandDiv.setAttribute('data-category', category);
        
        const hasArguments = cmd.arguments && cmd.arguments.length > 0;
        
        // Main command button with expand arrow
        const button = document.createElement('button');
        button.className = 'command-button w-full text-left p-3 border border-gray-200 rounded-md hover:bg-gray-50 transition-all duration-200 flex items-center justify-between';
        button.setAttribute('data-command', cmd.name);
        
        // Create button content with simple button integrated
        let buttonContent = `
            <div class="flex-1">
                <div class="font-medium text-gray-800 text-sm command-name">${cmd.name}</div>
                <div class="text-xs text-gray-600 mt-1">${cmd.description}</div>
            </div>
        `;
        
        if (hasArguments) {
            buttonContent += `
                <div class="flex items-center gap-2">
                    <button 
                        id="simple-${cmd.name.replace(/\s+/g, '-')}"
                        onclick="event.stopPropagation(); setActiveCommand(this.parentElement.parentElement); executeCommand('${cmd.name}')" 
                        class="simple-button text-gray-500 hover:text-blue-600 hover:bg-blue-50 px-2 py-1 rounded-full transition-all duration-200"
                        title="Run without parameters"
                    >
                        <i class="fas fa-play-circle text-lg"></i>
                    </button>
                    <i class="fas fa-chevron-right text-gray-400 text-xs expand-icon transition-transform duration-200"></i>
                </div>
            `;
        }
        
        button.innerHTML = buttonContent;
        
        // Create expandable arguments section
        const argsSection = document.createElement('div');
        argsSection.className = 'arguments-section hidden mt-2 p-3 bg-gray-50 border border-gray-200 rounded-md';
        argsSection.id = `args-${cmd.name.replace(/\s+/g, '-')}`;
        
        if (hasArguments) {
            let argsHTML = '<div class="text-sm font-medium text-gray-700 mb-3">Arguments:</div>';
            
            cmd.arguments.forEach(arg => {
                const argId = `${cmd.name.replace(/\s+/g, '-')}-${arg.name.replace(/^--/, '').replace(/\s+/g, '-')}`;
                const isFlag = arg.type === 'flag';
                const isEnum = arg.type === 'enum';
                const isRequired = arg.required === 'true';
                
                let inputHTML = '';
                
                if (isFlag) {
                    inputHTML = `<input type="checkbox" id="${argId}" class="argument-input" data-arg="${arg.name}" data-type="${arg.type}" ${arg.default ? `data-default="${arg.default}"` : ''} title="${arg.description}" ${arg.default === 'true' ? 'checked' : ''}>`;
                } else if (isEnum && arg.options) {
                    const options = arg.options.split(',');
                    inputHTML = `<div class="enum-selector" id="${argId}" data-arg="${arg.name}" data-type="${arg.type}" ${arg.default ? `data-default="${arg.default}"` : ''}>`;
                    options.forEach(option => {
                        const optionId = `${argId}-${option}`;
                        inputHTML += `<span class="enum-option ${arg.default === option ? 'selected' : ''}" data-value="${option}" onclick="toggleEnumOption('${argId}', '${option}')">${option}</span>`;
                    });
                    inputHTML += `<input type="hidden" id="${argId}-value" class="argument-input" data-arg="${arg.name}" data-type="${arg.type}" ${arg.default ? `value="${arg.default}"` : 'value=""'}>`;
                    inputHTML += `</div>`;
                } else {
                    inputHTML = `<div class="relative">
                        <input type="${arg.type === 'int' ? 'number' : 'text'}" id="${argId}" class="argument-input w-full ${clipboardAvailable ? 'pr-8' : 'pr-2'} pl-2 py-1 text-xs border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-500" data-arg="${arg.name}" data-type="${arg.type}" ${arg.default ? `data-default="${arg.default}"` : ''} placeholder="${arg.description}" title="${arg.description}" ${arg.default && arg.type !== 'flag' ? `value="${arg.default}"` : ''}>
                        ${clipboardAvailable ? `<button onclick="pasteToInput('${argId}')" class="paste-button" style="position: absolute; right: 0.375rem; top: 50%; transform: translateY(-50%); background: transparent; color: #9ca3af; padding: 0.125rem 0.25rem; border-radius: 0.25rem; transition: all 0.2s;" onmouseover="this.style.backgroundColor='rgba(156, 163, 175, 0.1)'; this.style.color='#6b7280';" onmouseout="this.style.backgroundColor='transparent'; this.style.color='#9ca3af';" title="Paste from clipboard">
                            <i class="fas fa-paste" style="font-size: 0.625rem;"></i>
                        </button>` : ''}
                    </div>`;
                }
                
                argsHTML += `
                    <div class="mb-3">
                        <label class="block text-xs font-medium text-gray-600 mb-1" for="${argId}">
                            ${arg.name} ${isRequired ? '<span class="text-red-500">*</span>' : ''}
                            ${arg.default ? `<span class="text-gray-400">(default: ${arg.default})</span>` : ''}
                        </label>
                        ${inputHTML}
                    </div>
                `;
            });
            
            argsHTML += `
                <div class="mt-4">
                    <button onclick="executeCommandWithArgs('${cmd.name}')" class="w-full bg-blue-600 text-white px-3 py-2 rounded text-xs hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500">
                        <i class="fas fa-play mr-1"></i>Execute with Parameters
                    </button>
                </div>
            `;
            
            argsSection.innerHTML = argsHTML;
            
            // Add click handler for expand/collapse
            button.onclick = (e) => {
                // Don't toggle if clicking on the simple button
                if (!e.target.closest('.simple-button')) {
                    toggleArgumentsSection(cmd.name, button);
                }
            };
        } else {
            // No arguments, execute directly
            button.onclick = () => {
                setActiveCommand(button);
                executeCommand(cmd.name);
            };
        }
        
        commandDiv.appendChild(button);
        if (hasArguments) {
            commandDiv.appendChild(argsSection);
        }
        container.appendChild(commandDiv);
    });
}

// Toggle arguments section visibility
function toggleArgumentsSection(commandName, button) {
    const argsSection = document.getElementById(`args-${commandName.replace(/\s+/g, '-')}`);
    const expandIcon = button.querySelector('.expand-icon');
    const simpleButton = document.getElementById(`simple-${commandName.replace(/\s+/g, '-')}`);
    
    if (argsSection.classList.contains('hidden')) {
        argsSection.classList.remove('hidden');
        if (expandIcon) {
            expandIcon.classList.add('rotate-90');
        }
        // Hide simple button when expanded
        if (simpleButton) {
            simpleButton.style.display = 'none';
        }
    } else {
        argsSection.classList.add('hidden');
        if (expandIcon) {
            expandIcon.classList.remove('rotate-90');
        }
        // Show simple button when collapsed
        if (simpleButton) {
            simpleButton.style.display = '';
        }
    }
}

// Execute command with arguments
async function executeCommandWithArgs(commandName) {
    const argsSection = document.getElementById(`args-${commandName.replace(/\s+/g, '-')}`);
    const inputs = argsSection.querySelectorAll('.argument-input');
    
    let command = commandName;
    let args = [];
    
    inputs.forEach(input => {
        const argName = input.getAttribute('data-arg');
        const argType = input.getAttribute('data-type');
        const defaultValue = input.getAttribute('data-default');
        
        if (argType === 'flag') {
            // For flags with default values:
            // - If has default="true" and unchecked, we need to explicitly set to false
            // - If has default="true" and checked, skip (use default)
            // - If no default and checked, include the flag
            if (defaultValue === 'true') {
                if (!input.checked) {
                    // Need to explicitly set to false
                    args.push(`${argName}=false`);
                }
                // If checked, don't add anything (will use default true)
            } else {
                // Normal flag behavior - only add if checked
                if (input.checked) {
                    args.push(argName);
                }
            }
        } else {
            const value = input.value.trim();
            // Only add non-flag arguments if they have a value and differ from default
            if (value && (defaultValue === undefined || value !== defaultValue)) {
                if (argName.startsWith('--')) {
                    args.push(`${argName}=${value}`);
                } else {
                    // Positional argument
                    args.push(value);
                }
            }
        }
    });
    
    if (args.length > 0) {
        command += ' ' + args.join(' ');
    }
    
    await executeCommand(command);
}

// Set active command button
function setActiveCommand(button) {
    // Remove active class from all command buttons
    document.querySelectorAll('.command-button').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Add active class to clicked button
    if (button) {
        button.classList.add('active');
        currentActiveCommand = button;
    }
}

// Execute a command
async function executeCommand(command) {
    if (!connectionState.connected) {
        showError('Please connect to etcd first');
        return;
    }
    
    showLoading(true, 'Executing command...');
    
    // Start a new command output box
    startCommandOutput(command);
    
    try {
        const response = await fetch('/api/command', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                command: command
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            let outputContent = '';
            
            if (result.output) {
                outputContent += result.output;
            }
            if (result.data && typeof result.data === 'object') {
                outputContent += JSON.stringify(result.data, null, 2) + '\n';
            } else if (result.data) {
                outputContent += result.data + '\n';
            }
            
            // If no output, show success message
            if (!result.output && !result.data) {
                outputContent = '✅ Command executed successfully (no output)\n';
            }
            
            appendToCommandOutput(outputContent, 'success');
        } else {
            let errorContent = `❌ Error: ${result.error}\n`;
            if (result.output) {
                errorContent += result.output;
            }
            appendToCommandOutput(errorContent, 'error');
        }
        
    } catch (error) {
        appendToCommandOutput(`❌ Network error: ${error.message}\n`, 'error');
    } finally {
        showLoading(false);
        // Ensure output scrolls to bottom after command execution
        scrollOutputToBottom();
        
        // Remove active state after command execution
        setTimeout(() => {
            if (currentActiveCommand) {
                currentActiveCommand.classList.remove('active');
                currentActiveCommand = null;
            }
        }, 1000);
    }
}

// Execute custom command
async function executeCustomCommand() {
    const commandInput = document.getElementById('custom-command');
    const command = commandInput.value.trim();
    
    if (!command) {
        showError('Please enter a command');
        return;
    }
    
    // Clear the input
    commandInput.value = '';
    
    // Execute the command
    await executeCommand(command);
}

// Show/hide loading indicator and disable/enable buttons
function showLoading(show, text = 'Executing...') {
    const loadingIndicator = document.getElementById('loading-indicator');
    const loadingText = document.getElementById('loading-text');
    
    if (show) {
        // Show loading indicator
        loadingText.textContent = text;
        loadingIndicator.classList.remove('hidden');
        
        // Disable all command buttons
        document.querySelectorAll('.command-button').forEach(btn => {
            btn.disabled = true;
            btn.classList.add('opacity-50', 'cursor-not-allowed');
        });
        
        // Disable simple buttons
        document.querySelectorAll('.simple-button').forEach(btn => {
            btn.disabled = true;
            btn.classList.add('opacity-50', 'cursor-not-allowed');
        });
        
        // Disable execute buttons in arguments sections
        document.querySelectorAll('button[onclick^="executeCommandWithArgs"]').forEach(btn => {
            btn.disabled = true;
            btn.classList.add('opacity-50', 'cursor-not-allowed');
        });
        
        // Disable custom command button
        const customBtn = document.querySelector('button[onclick="executeCustomCommand()"]');
        if (customBtn) {
            customBtn.disabled = true;
            customBtn.classList.add('opacity-50', 'cursor-not-allowed');
        }
        
        // Disable clear and copy buttons
        document.querySelectorAll('button[onclick="clearOutput()"], button[onclick="copyOutput()"]').forEach(btn => {
            btn.disabled = true;
            btn.classList.add('opacity-50', 'cursor-not-allowed');
        });
    } else {
        // Hide loading indicator
        loadingIndicator.classList.add('hidden');
        
        // Enable all command buttons
        document.querySelectorAll('.command-button').forEach(btn => {
            btn.disabled = false;
            btn.classList.remove('opacity-50', 'cursor-not-allowed');
        });
        
        // Enable simple buttons
        document.querySelectorAll('.simple-button').forEach(btn => {
            btn.disabled = false;
            btn.classList.remove('opacity-50', 'cursor-not-allowed');
        });
        
        // Enable execute buttons in arguments sections
        document.querySelectorAll('button[onclick^="executeCommandWithArgs"]').forEach(btn => {
            btn.disabled = false;
            btn.classList.remove('opacity-50', 'cursor-not-allowed');
        });
        
        // Enable custom command button
        const customBtn = document.querySelector('button[onclick="executeCustomCommand()"]');
        if (customBtn) {
            customBtn.disabled = false;
            customBtn.classList.remove('opacity-50', 'cursor-not-allowed');
        }
        
        // Enable clear and copy buttons
        document.querySelectorAll('button[onclick="clearOutput()"], button[onclick="copyOutput()"]').forEach(btn => {
            btn.disabled = false;
            btn.classList.remove('opacity-50', 'cursor-not-allowed');
        });
    }
}

// Global variable to track current command output box
let currentCommandBox = null;

// Start a new command output box
function startCommandOutput(command) {
    const output = document.getElementById('output');
    
    // Create command output box
    const commandBox = document.createElement('div');
    commandBox.className = 'command-output-box';
    
    // Create command header
    const header = document.createElement('div');
    header.className = 'command-header';
    header.innerHTML = `<span class="word-clickable" data-word="${escapeHtml(command)}">$ ${escapeHtml(command)}</span>`;
    
    // Create command content container
    const content = document.createElement('div');
    content.className = 'command-content';
    
    commandBox.appendChild(header);
    commandBox.appendChild(content);
    output.appendChild(commandBox);
    
    // Set as current command box
    currentCommandBox = content;
    
    scrollOutputToBottom();
}

// Append content to current command output box
function appendToCommandOutput(text, type = 'normal') {
    if (!currentCommandBox) {
        // Fallback to regular append if no command box is active
        appendOutput(text);
        return;
    }
    
    // Process the text to make words clickable
    const processedText = makeWordsClickable(text);
    
    // Create a temporary div to hold the processed HTML
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = processedText;
    
    // Apply styling based on type
    if (type === 'success') {
        tempDiv.classList.add('success-output');
    } else if (type === 'error') {
        tempDiv.classList.add('error-output');
    } else if (type === 'info') {
        tempDiv.classList.add('info-output');
    }
    
    // Append all child nodes to the current command box
    while (tempDiv.firstChild) {
        currentCommandBox.appendChild(tempDiv.firstChild);
    }
    
    scrollOutputToBottom();
}

// Append text to output and auto-scroll (for non-command output)
function appendOutput(text) {
    const output = document.getElementById('output');
    
    // Process the text to make words clickable
    const processedText = makeWordsClickable(text);
    
    // Create a temporary div to hold the processed HTML
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = processedText;
    
    // Append all child nodes to the output
    while (tempDiv.firstChild) {
        output.appendChild(tempDiv.firstChild);
    }
    
    scrollOutputToBottom();
}

// Process text to make individual words clickable
function makeWordsClickable(text) {
    // Split text by lines to preserve line breaks
    const lines = text.split('\n');
    
    return lines.map(line => {
        // Split each line by spaces, colons, equals, brackets, braces, commas, and quotes, but preserve the separators
        const parts = line.split(/(\s+|:|\(|\)|\[|\]|=|,|{|}|")/);
        
        return parts.map(part => {
            // If it's whitespace, return as-is
            if (/^\s+$/.test(part)) {
                return part;
            }
            
            // If it's a non-empty part (word or separator), make it clickable
            if (part.length > 0) {
                // Check if it's a timestamp
                const isTimestamp = isValidTimestamp(part);
                const extraClass = isTimestamp ? ' timestamp-value' : '';
                
                // Use data attribute instead of onclick to avoid quote issues
                return `<span class="word-clickable${extraClass}" data-word="${escapeHtml(part)}">${escapeHtml(part)}</span>`;
            }
            
            return part;
        }).join('');
    }).join('\n');
}

// Check if a string is a valid timestamp between 1900 and 2100
function isValidTimestamp(str) {
    // Check if it's all digits
    if (!/^\d+$/.test(str)) {
        return false;
    }
    
    // Check for Milvus hybrid timestamp (usually 18-19 digits)
    if (str.length >= 18 && str.length <= 19) {
        // Parse as Milvus timestamp using BigInt for accuracy
        const result = parseMilvusTSFromString(str);
        if (result) {
            const { physicalTime } = result;
            const minDate = new Date('1900-01-01').getTime();
            const maxDate = new Date('2100-01-01').getTime();
            return physicalTime >= minDate && physicalTime <= maxDate;
        }
    }
    
    // Check if it's a regular timestamp (10-13 digits)
    if (str.length < 10 || str.length > 13) {
        return false;
    }
    
    const num = parseInt(str);
    if (isNaN(num)) {
        return false;
    }
    
    // Convert to milliseconds if it's in seconds
    const timestamp = str.length === 10 ? num * 1000 : num;
    
    // Check if it's between 1900 and 2100
    const minDate = new Date('1900-01-01').getTime();
    const maxDate = new Date('2100-01-01').getTime();
    
    return timestamp >= minDate && timestamp <= maxDate;
}

// Parse Milvus hybrid timestamp from string using BigInt
function parseMilvusTSFromString(tsStr) {
    try {
        const ts = BigInt(tsStr);
        const logicalBits = 18n;
        const logicalBitsMask = (1n << logicalBits) - 1n;
        
        const logical = Number(ts & logicalBitsMask);
        const physical = Number(ts >> logicalBits);
        const physicalTime = physical; // in milliseconds
        
        return {
            physicalTime,
            logical,
            ts: tsStr
        };
    } catch (e) {
        return null;
    }
}

// Escape HTML characters for safe insertion
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Copy a single word to clipboard and show notification
async function copyWord(word) {
    let success = false;
    
    try {
        // First try the modern clipboard API
        if (navigator.clipboard && navigator.clipboard.writeText && window.isSecureContext) {
            await navigator.clipboard.writeText(word);
            success = true;
        }
    } catch (error) {
        console.log('Clipboard API failed, trying fallback:', error.message);
    }
    
    // If modern API failed, try fallback
    if (!success) {
        success = fallbackCopyToClipboard(word);
    }
    
    if (success) {
        showCopyNotification(word);
    } else {
        showCopyError();
    }
}

// Fallback copy method for older browsers
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
    try {
        success = document.execCommand('copy');
    } catch (err) {
        console.error('Fallback copy failed:', err);
    }
    
    document.body.removeChild(textArea);
    return success;
}

// Show copy error message
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
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 300);
    }, 2000);
}

// Show copy notification
function showCopyNotification(word) {
    // Remove any existing notification
    const existingNotification = document.querySelector('.copy-notification');
    if (existingNotification) {
        existingNotification.remove();
    }
    
    // Create new notification
    const notification = document.createElement('div');
    notification.className = 'copy-notification';
    notification.innerHTML = `<i class="fas fa-check mr-2"></i>Copied: "${word}"`;
    
    document.body.appendChild(notification);
    
    // Auto-hide after 1.5 seconds
    setTimeout(() => {
        notification.classList.add('hide');
        setTimeout(() => {
            if (notification.parentNode) {
                notification.parentNode.removeChild(notification);
            }
        }, 300); // Wait for animation to complete
    }, 1500);
}

// Global state for shift-click selection
let shiftSelectionState = {
    isActive: false,
    startElement: null,
    allClickableElements: []
};

// Initialize word click handlers using event delegation
function initializeWordClickHandlers() {
    const output = document.getElementById('output');
    if (output) {
        // Handle clicks
        output.addEventListener('click', function(event) {
            if (event.target.classList.contains('word-clickable')) {
                handleWordClick(event);
            }
        });
        
        // Handle mouse movement for preview during shift selection and timestamp tooltips
        output.addEventListener('mouseover', function(event) {
            if (event.target.classList.contains('word-clickable')) {
                if (shiftSelectionState.isActive) {
                    handleWordHover(event);
                }
                // Check if it's a timestamp
                if (event.target.classList.contains('timestamp-value')) {
                    showTimestampTooltip(event.target);
                }
            }
        });
        
        // Handle mouse leaving to clear preview and hide tooltips
        output.addEventListener('mouseout', function(event) {
            if (event.target.classList.contains('word-clickable')) {
                if (shiftSelectionState.isActive) {
                    clearSelectionPreview();
                }
                // Hide timestamp tooltip
                if (event.target.classList.contains('timestamp-value')) {
                    hideTimestampTooltip();
                }
            }
        });
    }
    
    // Listen for shift key events globally
    document.addEventListener('keyup', function(event) {
        if (event.key === 'Shift' && shiftSelectionState.isActive) {
            endShiftSelection();
        }
    });
    
    // Clear selection if user clicks outside while holding shift
    document.addEventListener('click', function(event) {
        if (shiftSelectionState.isActive && !event.target.classList.contains('word-clickable')) {
            // Do nothing - ignore clicks outside clickable elements as specified
        }
    });
}

// Handle word click events
function handleWordClick(event) {
    const element = event.target;
    const word = element.getAttribute('data-word');
    
    if (!word) return;
    
    if (event.shiftKey) {
        // Prevent default browser text selection behavior
        event.preventDefault();
        event.stopPropagation();
        handleShiftClick(element, word);
    } else {
        // Normal click - copy single word and clear any active selection
        if (shiftSelectionState.isActive) {
            endShiftSelection();
        }
        copyWord(word);
    }
}

// Handle shift+click functionality
function handleShiftClick(element, word) {
    if (!shiftSelectionState.isActive) {
        // Start new selection
        startShiftSelection(element);
    } else {
        // Complete selection or handle same element click
        if (element === shiftSelectionState.startElement) {
            // Clicking same element twice - copy just that element
            copyWord(word);
            endShiftSelection();
        } else {
            // Complete range selection
            completeRangeSelection(element);
        }
    }
}

// Start shift selection mode
function startShiftSelection(startElement) {
    shiftSelectionState.isActive = true;
    shiftSelectionState.startElement = startElement;
    
    // Get all clickable elements in the output for range calculation
    updateClickableElementsList();
    
    // Highlight start element
    startElement.classList.add('selection-start');
    
    // Disable text selection during shift mode
    document.body.style.userSelect = 'none';
    document.body.style.webkitUserSelect = 'none';
    document.body.style.mozUserSelect = 'none';
    document.body.style.msUserSelect = 'none';
}

// Update the list of all clickable elements
function updateClickableElementsList() {
    const output = document.getElementById('output');
    shiftSelectionState.allClickableElements = Array.from(output.querySelectorAll('.word-clickable'));
}

// Handle hover during shift selection for preview
function handleWordHover(event) {
    if (!shiftSelectionState.isActive) return;
    
    const hoveredElement = event.target;
    clearSelectionPreview();
    
    // Show preview of what would be selected
    const range = getElementRange(shiftSelectionState.startElement, hoveredElement);
    if (range) {
        range.forEach(el => {
            if (el !== shiftSelectionState.startElement) {
                el.classList.add('selection-preview');
            }
        });
    }
}

// Clear selection preview
function clearSelectionPreview() {
    shiftSelectionState.allClickableElements.forEach(el => {
        el.classList.remove('selection-preview');
    });
}

// Complete range selection and copy
function completeRangeSelection(endElement) {
    const range = getElementRange(shiftSelectionState.startElement, endElement);
    
    if (range && range.length > 0) {
        // Get the original text by extracting from DOM
        const selectedText = getOriginalTextFromRange(range);
        
        // Copy the selected text
        copyWord(selectedText);
        
        // Highlight the selected range briefly
        clearSelectionPreview();
        range.forEach(el => {
            el.classList.add('selection-range');
        });
        
        // Clear highlights after a short delay
        setTimeout(() => {
            range.forEach(el => {
                el.classList.remove('selection-range');
            });
        }, 1000);
    }
    
    endShiftSelection();
}

// Get range of elements between start and end (inclusive)
function getElementRange(startElement, endElement) {
    const startIndex = shiftSelectionState.allClickableElements.indexOf(startElement);
    const endIndex = shiftSelectionState.allClickableElements.indexOf(endElement);
    
    if (startIndex === -1 || endIndex === -1) return null;
    
    const minIndex = Math.min(startIndex, endIndex);
    const maxIndex = Math.max(startIndex, endIndex);
    
    return shiftSelectionState.allClickableElements.slice(minIndex, maxIndex + 1);
}

// Extract original text from a range of elements, preserving exact spacing and separators
function getOriginalTextFromRange(elements) {
    if (!elements || elements.length === 0) return '';
    
    // Find the common parent container
    const firstElement = elements[0];
    const lastElement = elements[elements.length - 1];
    
    // Get the parent that contains both elements
    let commonParent = firstElement.parentNode;
    while (commonParent && !commonParent.contains(lastElement)) {
        commonParent = commonParent.parentNode;
    }
    
    if (!commonParent) return elements.map(el => el.getAttribute('data-word')).join('');
    
    // Create a range to extract the exact text
    const range = document.createRange();
    range.setStartBefore(firstElement);
    range.setEndAfter(lastElement);
    
    // Get the text content, which preserves the original spacing
    return range.toString();
}

// End shift selection mode
function endShiftSelection() {
    if (!shiftSelectionState.isActive) return;
    
    // Clear all selection classes
    shiftSelectionState.allClickableElements.forEach(el => {
        el.classList.remove('selection-start', 'selection-preview', 'selection-range');
    });
    
    // Re-enable text selection
    document.body.style.userSelect = '';
    document.body.style.webkitUserSelect = '';
    document.body.style.mozUserSelect = '';
    document.body.style.msUserSelect = '';
    
    // Reset state
    shiftSelectionState.isActive = false;
    shiftSelectionState.startElement = null;
    shiftSelectionState.allClickableElements = [];
}

// Scroll output to bottom
function scrollOutputToBottom() {
    const output = document.getElementById('output');
    // Use requestAnimationFrame to ensure the scroll happens after DOM updates
    requestAnimationFrame(() => {
        output.scrollTop = output.scrollHeight;
    });
}

// Clear output
function clearOutput() {
    const output = document.getElementById('output');
    output.innerHTML = '';
    currentCommandBox = null; // Reset command box state
    appendOutput('Welcome to Birdwatcher Web UI!\nYou are now connected to your Milvus deployment.\nSelect a command from the left panel or enter a custom command to start debugging.\n');
}

// Copy output to clipboard
async function copyOutput() {
    const output = document.getElementById('output');
    const text = output.textContent || output.innerText;
    
    try {
        await navigator.clipboard.writeText(text);
        
        // Show temporary success message
        const copyButton = document.querySelector('button[onclick="copyOutput()"]');
        const originalText = copyButton.innerHTML;
        copyButton.innerHTML = '<i class="fas fa-check mr-1"></i>Copied!';
        copyButton.classList.remove('bg-blue-500', 'hover:bg-blue-600');
        copyButton.classList.add('bg-green-500', 'hover:bg-green-600');
        
        setTimeout(() => {
            copyButton.innerHTML = originalText;
            copyButton.classList.remove('bg-green-500', 'hover:bg-green-600');
            copyButton.classList.add('bg-blue-500', 'hover:bg-blue-600');
        }, 2000);
        
    } catch (error) {
        console.error('Failed to copy to clipboard:', error);
        showError('Failed to copy to clipboard');
    }
}

// Show error message
function showError(message) {
    startCommandOutput('error');
    appendToCommandOutput(`❌ ${message}\n`, 'error');
}

// Initialize resizer functionality
function initializeResizer() {
    const resizer = document.getElementById('resizer');
    const leftPanel = document.getElementById('left-panel');
    const rightPanel = document.getElementById('right-panel');
    const contentGrid = document.getElementById('content-grid');
    
    if (!resizer || !leftPanel || !rightPanel || !contentGrid) {
        return; // Elements not found, probably on connect screen
    }
    
    let isResizing = false;
    let startX = 0;
    let startLeftWidth = 0;
    let startRightWidth = 0;
    
    resizer.addEventListener('mousedown', function(e) {
        isResizing = true;
        startX = e.clientX;
        
        // Get current widths
        const leftRect = leftPanel.getBoundingClientRect();
        const rightRect = rightPanel.getBoundingClientRect();
        startLeftWidth = leftRect.width;
        startRightWidth = rightRect.width;
        
        // Prevent text selection during resize
        document.body.style.userSelect = 'none';
        document.body.style.cursor = 'col-resize';
        
        e.preventDefault();
    });
    
    document.addEventListener('mousemove', function(e) {
        if (!isResizing) return;
        
        const deltaX = e.clientX - startX;
        const containerWidth = contentGrid.getBoundingClientRect().width - 6; // Subtract resizer width
        
        // Calculate new widths
        let newLeftWidth = startLeftWidth + deltaX;
        let newRightWidth = startRightWidth - deltaX;
        
        // Set minimum widths (20% and 20%)
        const minWidth = containerWidth * 0.2;
        const maxLeftWidth = containerWidth * 0.8;
        
        if (newLeftWidth < minWidth) {
            newLeftWidth = minWidth;
            newRightWidth = containerWidth - newLeftWidth;
        } else if (newLeftWidth > maxLeftWidth) {
            newLeftWidth = maxLeftWidth;
            newRightWidth = containerWidth - newLeftWidth;
        }
        
        // Convert to percentages
        const leftPercent = (newLeftWidth / containerWidth) * 100;
        const rightPercent = (newRightWidth / containerWidth) * 100;
        
        // Apply new widths
        leftPanel.style.width = leftPercent + '%';
        rightPanel.style.width = rightPercent + '%';
        
        e.preventDefault();
    });
    
    document.addEventListener('mouseup', function() {
        if (isResizing) {
            isResizing = false;
            document.body.style.userSelect = '';
            document.body.style.cursor = '';
        }
    });
    
    // Handle double-click to reset to default 30/70 ratio
    resizer.addEventListener('dblclick', function() {
        leftPanel.style.width = '30%';
        rightPanel.style.width = '70%';
    });
}

// Global variable to track current tooltip
let currentTooltip = null;

// Show timestamp tooltip
function showTimestampTooltip(element) {
    const timestampStr = element.getAttribute('data-word');
    
    let timestampMs;
    let milvusInfo = '';
    
    // Check if it's a Milvus hybrid timestamp
    if (timestampStr.length >= 18 && timestampStr.length <= 19) {
        const result = parseMilvusTSFromString(timestampStr);
        if (result) {
            timestampMs = result.physicalTime;
            milvusInfo = `<div class="milvus-info">Milvus TS: ${timestampStr} (Logical: ${result.logical})</div>`;
        } else {
            // If parsing failed, treat as regular timestamp
            const timestamp = parseInt(timestampStr);
            if (isNaN(timestamp)) return;
            timestampMs = timestamp;
        }
    } else {
        // Parse regular timestamp
        const timestamp = parseInt(timestampStr);
        if (isNaN(timestamp)) return;
        // Convert to milliseconds if it's in seconds
        timestampMs = timestampStr.length === 10 ? timestamp * 1000 : timestamp;
    }
    
    // Create date object
    const date = new Date(timestampMs);
    
    // Format UTC time
    const utcString = date.toUTCString();
    
    // Format local time
    const localString = date.toLocaleString(undefined, {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZoneName: 'short'
    });
    
    // Calculate relative time if within 30 days
    const now = new Date();
    const diffMs = Math.abs(now - date);
    const diffDays = diffMs / (1000 * 60 * 60 * 24);
    
    let relativeTimeHtml = '';
    if (diffDays <= 30) {
        const relativeTime = formatRelativeTime(date, now);
        relativeTimeHtml = `<div class="relative-time">Relative: ${relativeTime}</div>`;
    }
    
    // Remove existing tooltip if any
    hideTimestampTooltip();
    
    // Create tooltip element
    const tooltip = document.createElement('div');
    tooltip.className = 'timestamp-tooltip';
    tooltip.innerHTML = `
        ${milvusInfo}
        <div class="utc-time">UTC: ${utcString}</div>
        <div class="local-time">Local: ${localString}</div>
        ${relativeTimeHtml}
    `;
    
    // Position relative to the element
    element.style.position = 'relative';
    element.appendChild(tooltip);
    
    // Store reference
    currentTooltip = tooltip;
    
    // Show tooltip with animation
    setTimeout(() => {
        tooltip.classList.add('show');
    }, 10);
}

// Format relative time with exactly two units of precision
function formatRelativeTime(date, now) {
    const diffMs = now - date;
    const isPast = diffMs > 0;
    const absDiffMs = Math.abs(diffMs);
    
    const seconds = Math.floor(absDiffMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);
    
    const units = [];
    
    if (days > 0) {
        units.push(`${days} day${days !== 1 ? 's' : ''}`);
        const remainingHours = hours % 24;
        if (remainingHours > 0) {
            units.push(`${remainingHours} hour${remainingHours !== 1 ? 's' : ''}`);
        } else {
            const remainingMinutes = minutes % 60;
            if (remainingMinutes > 0) {
                units.push(`${remainingMinutes} minute${remainingMinutes !== 1 ? 's' : ''}`);
            }
        }
    } else if (hours > 0) {
        units.push(`${hours} hour${hours !== 1 ? 's' : ''}`);
        const remainingMinutes = minutes % 60;
        if (remainingMinutes > 0) {
            units.push(`${remainingMinutes} minute${remainingMinutes !== 1 ? 's' : ''}`);
        } else {
            const remainingSeconds = seconds % 60;
            units.push(`${remainingSeconds} second${remainingSeconds !== 1 ? 's' : ''}`);
        }
    } else if (minutes > 0) {
        units.push(`${minutes} minute${minutes !== 1 ? 's' : ''}`);
        const remainingSeconds = seconds % 60;
        units.push(`${remainingSeconds} second${remainingSeconds !== 1 ? 's' : ''}`);
    } else {
        units.push(`${seconds} second${seconds !== 1 ? 's' : ''}`);
    }
    
    // Take only the first two units
    const timeString = units.slice(0, 2).join(' ');
    
    if (absDiffMs < 1000) {
        return 'just now';
    }
    
    return isPast ? `${timeString} ago` : `in ${timeString}`;
}

// Hide timestamp tooltip
function hideTimestampTooltip() {
    if (currentTooltip && currentTooltip.parentNode) {
        currentTooltip.classList.remove('show');
        setTimeout(() => {
            if (currentTooltip && currentTooltip.parentNode) {
                currentTooltip.parentNode.removeChild(currentTooltip);
            }
            currentTooltip = null;
        }, 200);
    }
}

// Toggle category visibility
function toggleCategory(category) {
    const toggle = document.getElementById(`${category}-toggle`);
    const container = document.getElementById(`${category}-commands`);
    
    categoryStates[category] = !categoryStates[category];
    
    if (categoryStates[category]) {
        container.style.display = 'block';
        toggle.classList.remove('collapsed');
    } else {
        container.style.display = 'none';
        toggle.classList.add('collapsed');
    }
}

// Filter commands based on search input
function filterCommands() {
    const searchInput = document.getElementById('command-search');
    const searchText = searchInput.value.toLowerCase().trim();
    
    if (!searchText) {
        // Show all commands and categories
        document.querySelectorAll('.command-item').forEach(item => {
            item.classList.remove('search-hidden');
            const nameElement = item.querySelector('.command-name');
            if (nameElement) {
                // Remove any existing highlights
                const originalName = nameElement.getAttribute('data-original-name') || nameElement.textContent;
                nameElement.innerHTML = originalName;
                nameElement.removeAttribute('data-original-name');
            }
        });
        
        // Show all category sections with their headers
        document.querySelectorAll('.category-section').forEach(section => {
            section.classList.remove('search-active');
            const commandsContainer = section.querySelector('[id$="-commands"]');
            if (commandsContainer) {
                commandsContainer.style.display = categoryStates[section.getAttribute('data-category')] ? 'block' : 'none';
            }
        });
        
        return;
    }
    
    // Hide all category headers when searching
    document.querySelectorAll('.category-section').forEach(section => {
        section.classList.add('search-active');
        const commandsContainer = section.querySelector('[id$="-commands"]');
        if (commandsContainer) {
            commandsContainer.style.display = 'block'; // Show all containers during search
        }
    });
    
    // Filter commands
    let hasVisibleCommands = false;
    document.querySelectorAll('.command-item').forEach(item => {
        const commandName = item.getAttribute('data-command-name').toLowerCase();
        const nameElement = item.querySelector('.command-name');
        
        if (commandName.includes(searchText)) {
            item.classList.remove('search-hidden');
            hasVisibleCommands = true;
            
            // Highlight matching text
            if (nameElement) {
                const originalName = nameElement.getAttribute('data-original-name') || nameElement.textContent;
                if (!nameElement.getAttribute('data-original-name')) {
                    nameElement.setAttribute('data-original-name', originalName);
                }
                
                // Create highlighted version
                const regex = new RegExp(`(${escapeRegExp(searchText)})`, 'gi');
                const highlightedName = originalName.replace(regex, '<span class="search-highlight">$1</span>');
                nameElement.innerHTML = highlightedName;
            }
        } else {
            item.classList.add('search-hidden');
        }
    });
}

// Helper function to escape special regex characters
function escapeRegExp(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Autocomplete functionality
let autocompleteState = {
    isActive: false,
    suggestions: [],
    selectedIndex: -1,
    container: null,
    input: null
};

// Initialize autocomplete
function initializeAutocomplete() {
    autocompleteState.input = document.getElementById('custom-command');
    autocompleteState.container = document.getElementById('autocomplete-container');
    
    if (!autocompleteState.input || !autocompleteState.container) return;
    
    // Handle input events
    autocompleteState.input.addEventListener('input', handleAutocompleteInput);
    autocompleteState.input.addEventListener('keydown', handleAutocompleteKeydown);
    autocompleteState.input.addEventListener('blur', () => {
        // Delay hiding to allow click events on suggestions
        setTimeout(() => hideAutocomplete(), 200);
    });
}

// Handle input changes for autocomplete
function handleAutocompleteInput(event) {
    const input = event.target.value.trim();
    
    if (!input) {
        hideAutocomplete();
        return;
    }
    
    const suggestions = generateSuggestions(input);
    if (suggestions.length > 0) {
        showAutocomplete(suggestions);
    } else {
        hideAutocomplete();
    }
}

// Generate suggestions based on input
function generateSuggestions(input) {
    const suggestions = [];
    const parts = input.split(/\s+/);
    const lastPart = parts[parts.length - 1];
    
    // If input ends with space and has arguments, suggest arguments
    if (input.endsWith(' ') && parts.length >= 1) {
        const commandPart = parts.slice(0, -1).join(' ');
        const command = findExactCommand(commandPart);
        
        if (command && command.arguments) {
            // Suggest arguments
            command.arguments.forEach(arg => {
                if (!parts.includes(arg.name)) {
                    suggestions.push({
                        type: 'argument',
                        command: commandPart,
                        text: arg.name,
                        description: arg.description,
                        display: `${commandPart} ${arg.name}`
                    });
                }
            });
        }
    } else if (lastPart.startsWith('--')) {
        // Suggesting specific arguments
        const commandPart = parts.slice(0, -1).join(' ');
        const command = findExactCommand(commandPart);
        
        if (command && command.arguments) {
            const argPrefix = lastPart.substring(2).toLowerCase();
            command.arguments.forEach(arg => {
                const argName = arg.name.substring(2).toLowerCase();
                if (argName.startsWith(argPrefix) && !parts.slice(0, -1).includes(arg.name)) {
                    suggestions.push({
                        type: 'argument',
                        command: commandPart,
                        text: arg.name,
                        description: arg.description,
                        display: `${commandPart} ${arg.name}`
                    });
                }
            });
        }
    } else {
        // Suggest commands
        const commandPrefix = input.toLowerCase();
        
        Object.entries(allCommandsData).forEach(([category, commands]) => {
            commands.forEach(cmd => {
                if (cmd.name.toLowerCase().startsWith(commandPrefix)) {
                    suggestions.push({
                        type: 'command',
                        category: category,
                        text: cmd.name,
                        description: cmd.description,
                        display: cmd.name
                    });
                }
            });
        });
    }
    
    return suggestions.slice(0, 10); // Limit to 10 suggestions
}

// Find exact command match
function findExactCommand(commandName) {
    for (const [category, commands] of Object.entries(allCommandsData)) {
        const command = commands.find(cmd => cmd.name === commandName);
        if (command) {
            return command;
        }
    }
    return null;
}

// Show autocomplete suggestions
function showAutocomplete(suggestions) {
    autocompleteState.suggestions = suggestions;
    autocompleteState.selectedIndex = -1;
    autocompleteState.isActive = true;
    
    // Clear container
    autocompleteState.container.innerHTML = '';
    
    // Add suggestions
    suggestions.forEach((suggestion, index) => {
        const item = document.createElement('div');
        item.className = 'autocomplete-item';
        
        if (suggestion.type === 'command') {
            item.innerHTML = `
                <span class="command-part">${escapeHtml(suggestion.text)}</span>
                <span class="description-part">${escapeHtml(suggestion.description)}</span>
            `;
        } else {
            item.innerHTML = `
                <span class="argument-part">${escapeHtml(suggestion.text)}</span>
                <span class="description-part">${escapeHtml(suggestion.description)}</span>
            `;
        }
        
        item.addEventListener('click', () => {
            selectAutocompleteSuggestion(index);
        });
        
        item.addEventListener('mouseenter', () => {
            updateSelectedIndex(index);
        });
        
        autocompleteState.container.appendChild(item);
    });
    
    // Position the container
    const inputRect = autocompleteState.input.getBoundingClientRect();
    const containerRect = autocompleteState.input.parentElement.getBoundingClientRect();
    autocompleteState.container.style.top = (inputRect.height + 4) + 'px';
    autocompleteState.container.style.left = '0';
    autocompleteState.container.style.right = '0';
    
    autocompleteState.container.classList.add('show');
}

// Hide autocomplete
function hideAutocomplete() {
    autocompleteState.isActive = false;
    autocompleteState.suggestions = [];
    autocompleteState.selectedIndex = -1;
    if (autocompleteState.container) {
        autocompleteState.container.classList.remove('show');
        autocompleteState.container.innerHTML = '';
    }
}

// Handle keyboard navigation
function handleAutocompleteKeydown(event) {
    if (!autocompleteState.isActive) {
        // Special handling for Enter key when no autocomplete
        if (event.key === 'Enter') {
            executeCustomCommand();
        }
        return;
    }
    
    switch (event.key) {
        case 'ArrowDown':
            event.preventDefault();
            updateSelectedIndex(
                (autocompleteState.selectedIndex + 1) % autocompleteState.suggestions.length
            );
            break;
            
        case 'ArrowUp':
            event.preventDefault();
            updateSelectedIndex(
                autocompleteState.selectedIndex <= 0 
                    ? autocompleteState.suggestions.length - 1 
                    : autocompleteState.selectedIndex - 1
            );
            break;
            
        case 'Tab':
            event.preventDefault();
            if (autocompleteState.selectedIndex >= 0) {
                selectAutocompleteSuggestion(autocompleteState.selectedIndex);
            } else if (autocompleteState.suggestions.length > 0) {
                selectAutocompleteSuggestion(0);
            }
            break;
            
        case 'Enter':
            if (autocompleteState.selectedIndex >= 0) {
                event.preventDefault();
                selectAutocompleteSuggestion(autocompleteState.selectedIndex);
            } else {
                hideAutocomplete();
                executeCustomCommand();
            }
            break;
            
        case 'Escape':
            event.preventDefault();
            hideAutocomplete();
            break;
    }
}

// Update selected index
function updateSelectedIndex(index) {
    const items = autocompleteState.container.querySelectorAll('.autocomplete-item');
    
    // Remove previous selection
    if (autocompleteState.selectedIndex >= 0) {
        items[autocompleteState.selectedIndex].classList.remove('selected');
    }
    
    // Set new selection
    autocompleteState.selectedIndex = index;
    if (index >= 0 && index < items.length) {
        items[index].classList.add('selected');
        // Ensure selected item is visible
        items[index].scrollIntoView({ block: 'nearest' });
    }
}

// Select autocomplete suggestion
function selectAutocompleteSuggestion(index) {
    const suggestion = autocompleteState.suggestions[index];
    if (!suggestion) return;
    
    const input = autocompleteState.input;
    const currentValue = input.value.trim();
    
    if (suggestion.type === 'command') {
        input.value = suggestion.text + ' ';
    } else if (suggestion.type === 'argument') {
        const parts = currentValue.split(/\s+/);
        if (currentValue.endsWith(' ')) {
            input.value = currentValue + suggestion.text + ' ';
        } else {
            parts[parts.length - 1] = suggestion.text;
            input.value = parts.join(' ') + ' ';
        }
    }
    
    hideAutocomplete();
    input.focus();
    
    // Trigger input event to potentially show new suggestions
    input.dispatchEvent(new Event('input', { bubbles: true }));
}

// Toggle enum option selection
function toggleEnumOption(selectorId, value) {
    const selector = document.getElementById(selectorId);
    const hiddenInput = document.getElementById(`${selectorId}-value`);
    const option = selector.querySelector(`[data-value="${value}"]`);
    
    if (!selector || !hiddenInput || !option) return;
    
    // If already selected, deselect
    if (option.classList.contains('selected')) {
        option.classList.remove('selected');
        hiddenInput.value = '';
    } else {
        // Remove previous selection
        selector.querySelectorAll('.enum-option').forEach(opt => {
            opt.classList.remove('selected');
        });
        
        // Select new option
        option.classList.add('selected');
        hiddenInput.value = value;
    }
} 