# Birdwatcher Web UI

A modern web interface for the Birdwatcher debugging tool for Milvus 2.0+.

## Features

- **Clean Connect Screen**: Easy connection to etcd with customizable address and root path
- **2-Column Layout**: Efficient workspace with command selection on the left and results on the right
- **Command Categories**: Organized commands into Show, Management, and Analysis categories with visual indicators
- **Interactive Interface**: Click-to-execute commands with visual feedback and real-time output
- **Custom Commands**: Execute any birdwatcher command through a dedicated input field
- **Real-time Output**: Terminal-style output display with command history and proper formatting
- **Enhanced UX**: Copy-to-clipboard functionality, active command highlighting, and improved visual design
- **Responsive Design**: Modern UI built with Tailwind CSS that works on desktop and mobile

## Usage

### Starting the Web UI

Use the `startweb` flag with the main birdwatcher command:

```bash
# Start web UI on default port (8002)
./birdwatcher -startweb

# Start web UI on custom port
./birdwatcher -startweb -port 8080
```

Or use the standalone web UI binary:

```bash
# Build the web UI
go build -o bin/birdwatcher-webui cmd/webui/main.go

# Start the web UI
./bin/birdwatcher-webui --port 8080
```

### Accessing the Interface

1. Open your browser and navigate to `http://localhost:8002` (or your custom port)
2. Enter your etcd connection details:
   - **etcd Address**: Default is `localhost:2379`
   - **Root Path**: Default is `by-dev`
3. Click "Connect to Milvus"
4. Once connected, you'll see the 2-column interface:
   - **Left Panel**: Command selection organized by categories
   - **Right Panel**: Command results and output

### Using the Interface

#### Command Selection (Left Panel)
- **Show Commands**: Green-highlighted section with data inspection commands
- **Management Commands**: Orange-highlighted section with administrative commands  
- **Analysis Commands**: Purple-highlighted section with analysis and debugging tools
- **Custom Command**: Red-highlighted section with a text input for any birdwatcher command

#### Results Panel (Right Panel)
- Real-time command output in terminal style
- **Clear** button to reset the output
- **Copy** button to copy all output to clipboard
- Automatic scrolling to show latest results
- Visual feedback for command execution status

#### Interactive Features
- Click any command button to execute it immediately
- Commands highlight when clicked and show execution status
- Custom commands can be executed by typing and pressing Enter or clicking Execute
- All output is captured and displayed in real-time

## API Endpoints

The web UI provides the following REST API endpoints:

- `GET /` - Main web interface
- `GET /api/status` - Server and connection status
- `GET /api/commands` - Available command categories
- `POST /api/connect` - Connect to etcd
- `POST /api/command` - Execute birdwatcher commands

## Command Categories

### Show Commands
- `show collections` - List available collections
- `show segments` - Display segment information
- `show session` - List online Milvus components
- `show replica` - List replica information
- `show index` - Show index information
- `show config-etcd` - List etcd configurations
- And many more...

### Management Commands
- `backup` - Backup etcd data
- `load-backup` - Load etcd backup file
- `kill` - Kill components

### Analysis Commands
- `storage-analysis` - Segment storage analysis
- `parse-ts` - Parse hybrid timestamps
- `validate-indexfiles` - Validate index file sizes

## Architecture

The web UI is built as a Go package that integrates directly with the birdwatcher framework:

- **Backend**: Go with Gin web framework
- **Frontend**: HTML/CSS/JavaScript with Tailwind CSS
- **Integration**: Direct integration with birdwatcher's state management and command processing
- **Output Capture**: Advanced stdout/stderr redirection to capture all command output
- **Static Files**: Embedded using Go's embed directive

## Development

The web UI package is located in the `webui/` directory and includes:

- `server.go` - Main web server implementation with output capture
- `static/index.html` - Web interface HTML with 2-column layout
- `static/app.js` - Frontend JavaScript functionality with enhanced UX

To modify the web UI, edit the files in the `webui/static/` directory and rebuild the birdwatcher binary.

## Recent Improvements

- **2-Column Layout**: Redesigned interface for better workflow efficiency
- **Output Capture**: Fixed command output capture to show actual results instead of generic success messages
- **Visual Feedback**: Added active command highlighting and execution status indicators
- **Copy Functionality**: Added clipboard integration for easy output sharing
- **Improved Styling**: Enhanced visual design with category color coding and hover effects
- **Better UX**: Streamlined command execution flow with automatic input clearing and status feedback 