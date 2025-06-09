package webui

import (
	"context"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states"
)

//go:embed static/*
var staticFiles embed.FS

const sessionTimeout = 30 * time.Minute

type Session struct {
	id         string
	state      framework.State
	connected  bool
	etcdAddr   string
	rootPath   string
	lastActive time.Time
	config     *configs.Config
}

func NewSession(config *configs.Config) *Session {
	return &Session{
		id:         uuid.New().String(),
		state:      states.Start(config, false),
		connected:  false,
		lastActive: time.Now(),
		config:     config,
	}
}

type WebApp struct {
	config *configs.Config
	// sessions map[sessionID]*Session
	sessions map[string]*Session
	mu       sync.RWMutex
	// execMu serializes command execution
	execMu sync.Mutex
}

type ConnectionRequest struct {
	EtcdAddr string `json:"etcd_addr"`
	RootPath string `json:"root_path"`
}

type CommandRequest struct {
	Command string `json:"command"`
}

type CommandResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Output  string      `json:"output,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type ConnectionStatus struct {
	Connected bool   `json:"connected"`
	EtcdAddr  string `json:"etcd_addr,omitempty"`
	RootPath  string `json:"root_path,omitempty"`
	Error     string `json:"error,omitempty"`
}

// StartWebUI starts the web UI server
func StartWebUI(config *configs.Config, port int) error {
	app := &WebApp{
		config:   config,
		sessions: make(map[string]*Session),
	}

	go app.cleanupSessions()

	r := gin.Default()

	// Enable CORS
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"*"},
		ExposeHeaders:    []string{"*"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	r.Use(func(c *gin.Context) {
		c.Set("app", app)
	})

	// Serve embedded static files
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		return fmt.Errorf("failed to create static file system: %w", err)
	}
	r.StaticFS("/static", http.FS(staticFS))
	
	// Serve index.html at root
	r.GET("/", func(c *gin.Context) {
		data, err := staticFiles.ReadFile("static/index.html")
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to read index.html")
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", data)
	})

	// Serve app.js
	r.GET("/app.js", func(c *gin.Context) {
		data, err := staticFiles.ReadFile("static/app.js")
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to read app.js")
			return
		}
		c.Data(http.StatusOK, "application/javascript", data)
	})

	// API routes
	api := r.Group("/api")
	api.Use(app.sessionMiddleware())
	{
		api.POST("/connect", app.handleConnect)
		api.POST("/command", app.handleCommand)
		api.GET("/status", app.handleStatus)
		api.GET("/commands", app.handleGetCommands)
		api.POST("/disconnect", app.handleDisconnect)
	}

	fmt.Printf("Birdwatcher Web UI starting on :%d\n", port)
	fmt.Printf("Open http://localhost:%d in your browser\n", port)
	
	return r.Run(fmt.Sprintf(":%d", port))
}

func (app *WebApp) cleanupSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		app.mu.Lock()
		for id, session := range app.sessions {
			if time.Since(session.lastActive) > sessionTimeout {
				// Properly close the state if it exists
				if session.state != nil {
					session.state.Close()
				}
				delete(app.sessions, id)
				fmt.Printf("Session %s expired and cleaned up\n", id)
			}
		}
		app.mu.Unlock()
	}
}

func (app *WebApp) sessionMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		var session *Session
		cookie, err := c.Cookie("session_id")

		app.mu.RLock()
		if err == nil && cookie != "" {
			session, _ = app.sessions[cookie]
		}
		app.mu.RUnlock()

		if session == nil {
			app.mu.Lock()
			// Double check after getting write lock
			if err == nil && cookie != "" {
				session, _ = app.sessions[cookie]
			}
			if session == nil {
				session = NewSession(app.config)
				app.sessions[session.id] = session
				fmt.Printf("Created new session %s\n", session.id)
			}
			app.mu.Unlock()
		}

		// Update last active time with proper locking
		app.mu.Lock()
		session.lastActive = time.Now()
		app.mu.Unlock()
		
		c.SetCookie("session_id", session.id, int(sessionTimeout.Seconds()), "/", "", false, true)
		c.Set("session", session)
		c.Next()
	}
}

func (app *WebApp) handleConnect(c *gin.Context) {
	session := c.MustGet("session").(*Session)
	var req ConnectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Use default values if not provided
	fmt.Printf("Connecting to etcd at %s\n", req.EtcdAddr)
	if req.EtcdAddr == "" {
		req.EtcdAddr = "localhost:2379"
	}
	// Don't set default root path if using auto mode
	if req.RootPath == "" {
		req.RootPath = "by-dev"
	}

	// Try to connect using birdwatcher framework
	var connectCmd string
	if req.RootPath == "__auto__" {
		// Use auto mode
		connectCmd = fmt.Sprintf("connect --etcd=%s --auto", req.EtcdAddr)
	} else {
		connectCmd = fmt.Sprintf("connect --etcd=%s --rootPath=%s", req.EtcdAddr, req.RootPath)
	}
	
	app.execMu.Lock()
	defer app.execMu.Unlock()
	newState, err := session.state.Process(connectCmd)
	
	if err != nil {
		// Extract meaningful error message
		errorMsg := err.Error()
		if strings.Contains(errorMsg, "context deadline exceeded") {
			errorMsg = "Connection timeout: Unable to reach etcd server. Please check the address and network connectivity."
		} else if strings.Contains(errorMsg, "connection refused") {
			errorMsg = "Connection refused: etcd server is not running or not accessible at the specified address."
		} else if strings.Contains(errorMsg, "no such host") {
			errorMsg = "Invalid hostname: The specified etcd address could not be resolved."
		}
		
		c.JSON(http.StatusOK, ConnectionStatus{
			Connected: false,
			Error:     errorMsg,
		})
		return
	}

	// Update the app state
	session.state = newState
	session.state.SetupCommands()
	session.connected = true
	session.etcdAddr = req.EtcdAddr
	session.rootPath = req.RootPath

	c.JSON(http.StatusOK, ConnectionStatus{
		Connected: true,
		EtcdAddr:  req.EtcdAddr,
		RootPath:  req.RootPath,
	})
}

func (app *WebApp) handleCommand(c *gin.Context) {
	session := c.MustGet("session").(*Session)
	var req CommandRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Check if we have a connected state
	if !session.connected || session.state == nil {
		c.JSON(http.StatusOK, CommandResponse{
			Success: false,
			Error:   "Not connected to etcd. Please connect first.",
		})
		return
	}

	// Execute the command using birdwatcher framework
	ctx := context.Background()
	
	// Try to execute the command
	app.execMu.Lock()
	defer app.execMu.Unlock()
	capturedOutput, err := app.executeCommandWithCapture(ctx, session, req.Command)
	
	response := CommandResponse{
		Success: err == nil,
		Output:  capturedOutput,
	}

	if err != nil {
		// Provide more context for specific errors
		errorMsg := err.Error()
		if strings.Contains(errorMsg, "not connected") || strings.Contains(errorMsg, "connection lost") {
			errorMsg = "Connection lost. Please reconnect to etcd."
			session.connected = false
		} else if strings.Contains(errorMsg, "permission denied") {
			errorMsg = "Permission denied. You may not have access to perform this operation."
		} else if strings.Contains(errorMsg, "not found") {
			errorMsg = "Resource not found. The requested item may have been deleted or does not exist."
		}
		response.Error = errorMsg
	}

	c.JSON(http.StatusOK, response)
}

func (app *WebApp) executeCommandWithCapture(ctx context.Context, session *Session, command string) (string, error) {
	// Save the original stdout and stderr
	originalStdout := os.Stdout
	originalStderr := os.Stderr
	
	// Create pipes to capture output
	rOut, wOut, err := os.Pipe()
	if err != nil {
		return "", fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	defer rOut.Close()
	
	rErr, wErr, err := os.Pipe()
	if err != nil {
		wOut.Close()
		return "", fmt.Errorf("failed to create stderr pipe: %w", err)
	}
	defer rErr.Close()
	
	// Redirect stdout and stderr to our pipes
	os.Stdout = wOut
	os.Stderr = wErr
	
	// Ensure we restore stdout/stderr even if panic occurs
	defer func() {
		os.Stdout = originalStdout
		os.Stderr = originalStderr
	}()
	
	// Channels to collect output with buffer
	stdoutDone := make(chan string, 1)
	stderrDone := make(chan string, 1)
	
	// Start goroutines to read from the pipes
	go func() {
		output, _ := io.ReadAll(rOut)
		stdoutDone <- string(output)
	}()
	
	go func() {
		output, _ := io.ReadAll(rErr)
		stderrDone <- string(output)
	}()
	
	// Execute the command
	newState, err := session.state.Process(command)
	
	// Close the write ends
	wOut.Close()
	wErr.Close()
	
	// Get the captured output with timeout
	capturedStdout := <-stdoutDone
	capturedStderr := <-stderrDone
	
	// Combine stdout and stderr
	var combinedOutputBuilder strings.Builder
	combinedOutputBuilder.WriteString(capturedStdout)
	if capturedStderr != "" {
		if combinedOutputBuilder.Len() > 0 {
			combinedOutputBuilder.WriteString("\n")
		}
		combinedOutputBuilder.WriteString(capturedStderr)
	}
	combinedOutput := combinedOutputBuilder.String()
	
	// Update state if it changed
	if newState != nil {
		session.state = newState
		session.state.SetupCommands()
	}
	
	if err != nil {
		return combinedOutput, err
	}
	
	// Return the captured output as the result
	return combinedOutput, nil
}

func (app *WebApp) handleStatus(c *gin.Context) {
	session := c.MustGet("session").(*Session)
	// Check actual connection health instead of just returning stored value
	actuallyConnected := session.connected
	if session.connected && session.state != nil {
		// Try a simple operation to test connectivity
		// We'll use a lightweight command that should work if connected
		app.execMu.Lock()
		_, err := app.executeCommandWithCapture(context.Background(), session, "show current-version")
		app.execMu.Unlock()
		if err != nil {
			// Connection appears to be lost, update the status
			session.connected = false
			actuallyConnected = false
		}
	}
	
	c.JSON(http.StatusOK, gin.H{
		"status":    "running",
		"connected": actuallyConnected,
		"time":      time.Now().Format(time.RFC3339),
	})
}

func (app *WebApp) handleGetCommands(c *gin.Context) {
	commands := map[string]interface{}{
		"show": []map[string]interface{}{
			{
				"name": "show collections",
				"description": "List current available collection from RootCoord",
				"usage": "show collections",
				"arguments": []map[string]string{
					{"name": "--dbid", "type": "int", "description": "database id to filter", "default": "-1"},
					{"name": "--id", "type": "int", "description": "collection id to display"},
					{"name": "--name", "type": "string", "description": "collection name to display"},
					{"name": "--state", "type": "string", "description": "collection state to filter"},
				},
			},
			{
				"name": "show segment",
				"description": "Display segment information from data coord meta store",
				"usage": "show segment",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to filter with"},
					{"name": "--detail", "type": "flag", "description": "flags indicating whether printing detail binlog info"},
					{"name": "--format", "type": "string", "description": "segment display format", "default": "line"},
					{"name": "--level", "type": "string", "description": "target segment level"},
					{"name": "--partition", "type": "int", "description": "partition id to filter with"},
					{"name": "--segment", "type": "int", "description": "segment id to display"},
					{"name": "--state", "type": "string", "description": "target segment state"},
				},
			},
			{
				"name": "show segment-index",
				"description": "Display segment index information",
				"usage": "show segment-index",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to filter with"},
					{"name": "--field", "type": "int", "description": "field id to filter with"},
					{"name": "--indexID", "type": "int", "description": "index id to filter with"},
					{"name": "--segment", "type": "int", "description": "segment id to filter with"},
				},
			},
			{
				"name": "show segment-loaded-grpc",
				"description": "List segments loaded information",
				"usage": "show segment-loaded-grpc",
				"arguments": []map[string]string{},
			},
			{
				"name": "show index",
				"description": "Display index information",
				"usage": "show index",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to list index on"},
				},
			},
			{
				"name": "show alias",
				"description": "List alias meta info",
				"usage": "show alias",
				"arguments": []map[string]string{
					{"name": "--dbid", "type": "int", "description": "database id to filter with", "default": "-1"},
				},
			},
			{
				"name": "show bulkinsert",
				"description": "Display bulkinsert jobs and tasks",
				"usage": "show bulkinsert",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to filter with"},
					{"name": "--detail", "type": "flag", "description": "flags indicating whether printing detail bulkinsert job"},
					{"name": "--job", "type": "int", "description": "job id to filter with"},
					{"name": "--showAllFiles", "type": "flag", "description": "flags indicating whether printing all files"},
					{"name": "--state", "type": "string", "description": "target import job state, [pending, preimporting, importing, failed, completed]"},
				},
			},
			{
				"name": "show channel-watch",
				"description": "Display channel watching info from data coord meta store",
				"usage": "show channel-watch",
				"arguments": []map[string]string{},
			},
			{
				"name": "show checkpoint",
				"description": "List checkpoint collection vchannels",
				"usage": "show checkpoint",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to filter with"},
				},
			},
			{
				"name": "show collection-loaded",
				"description": "Display information of loaded collection from querycoord",
				"usage": "show collection-loaded",
				"arguments": []map[string]string{},
			},
			{
				"name": "show compactions",
				"description": "List current available compactions from DataCoord",
				"usage": "show compactions",
				"arguments": []map[string]string{
					{"name": "--collectionID", "type": "int", "description": "collection id to filter"},
					{"name": "--collectionName", "type": "string", "description": "collection name to display"},
					{"name": "--detail", "type": "flag", "description": "flags indicating whether printing input/result segmentIDs"},
					{"name": "--ignoreDone", "type": "flag", "description": "ignore finished compaction tasks", "default": "true"},
					{"name": "--partitionID", "type": "int", "description": "partitionID id to filter"},
					{"name": "--planID", "type": "int", "description": "PlanID to filter"},
					{"name": "--segmentID", "type": "int", "description": "SegmentID to filter"},
					{"name": "--state", "type": "string", "description": "compaction state to filter"},
					{"name": "--triggerID", "type": "int", "description": "TriggerID to filter"},
				},
			},
			{
				"name": "show config-etcd",
				"description": "List configurations set by etcd source",
				"usage": "show config-etcd",
				"arguments": []map[string]string{},
			},
			{
				"name": "show configurations",
				"description": "Iterate all online components and inspect configuration",
				"usage": "show configurations",
				"arguments": []map[string]string{},
			},
			{
				"name": "show current-version",
				"description": "Display current Milvus Meta data version",
				"usage": "show current-version",
				"arguments": []map[string]string{},
			},
			{
				"name": "show database",
				"description": "Display Database info from rootcoord meta",
				"usage": "show database",
				"arguments": []map[string]string{},
			},
			{
				"name": "show etcd-kv-tree",
				"description": "Show etcd kv tree with key size of each prefix",
				"usage": "show etcd-kv-tree",
				"arguments": []map[string]string{},
			},
			{
				"name": "show partition",
				"description": "List partitions of provided collection",
				"usage": "show partition",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to list"},
				},
			},
			{
				"name": "show partition-loaded",
				"description": "Display the information of loaded partition(s) from querycoord meta",
				"usage": "show partition-loaded",
				"arguments": []map[string]string{},
			},
			{
				"name": "show replica",
				"description": "List current replica information from QueryCoord",
				"usage": "show replica",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to filter with"},
				},
			},
			{
				"name": "show resource-group",
				"description": "List resource groups in current instance",
				"usage": "show resource-group",
				"arguments": []map[string]string{},
			},
			{
				"name": "show session",
				"description": "List online milvus components",
				"usage": "show session",
				"arguments": []map[string]string{},
			},
			{
				"name": "show user",
				"description": "Display user info from rootcoord meta",
				"usage": "show user",
				"arguments": []map[string]string{},
			},
		},
		"management": []map[string]interface{}{
			{
				"name": "backup",
				"description": "Backup etcd key-values",
				"usage": "backup",
				"arguments": []map[string]string{
					{"name": "--batchSize", "type": "int", "description": "batch fetch size for etcd backup operation", "default": "100"},
					{"name": "--ignoreRevision", "type": "flag", "description": "backup ignore revision change, ONLY shall works with no nodes online"},
				},
			},
			{
				"name": "load-backup",
				"description": "Load etcd backup file",
				"usage": "load-backup [file]",
				"arguments": []map[string]string{
					{"name": "file", "type": "string", "description": "backup file path", "required": "true"},
					{"name": "--use-workspace", "type": "flag", "description": "use workspace"},
					{"name": "--workspace-name", "type": "string", "description": "workspace name"},
				},
			},
			{
				"name": "kill",
				"description": "Kill component session from etcd",
				"usage": "kill",
				"arguments": []map[string]string{
					{"name": "--component", "type": "string", "description": "component type to kill", "default": "ALL"},
					{"name": "--id", "type": "int", "description": "Server ID to kill"},
				},
			},
			{
				"name": "connect",
				"description": "Connect to etcd",
				"usage": "connect",
				"arguments": []map[string]string{
					{"name": "--etcd", "type": "string", "description": "etcd address"},
					{"name": "--rootPath", "type": "string", "description": "root path"},
				},
			},
			{
				"name": "disconnect",
				"description": "Disconnect from etcd",
				"usage": "disconnect",
				"arguments": []map[string]string{},
			},
		},
		"analysis": []map[string]interface{}{
			{
				"name": "storage-analysis",
				"description": "Segment storage analysis",
				"usage": "storage-analysis",
				"arguments": []map[string]string{
					{"name": "--collection", "type": "int", "description": "collection id to analysis"},
					{"name": "--detail", "type": "flag", "description": "print detailed binlog size info"},
				},
			},
			{
				"name": "parse-ts",
				"description": "Parse hybrid timestamp",
				"usage": "parse-ts",
				"arguments": []map[string]string{},
			},
			{
				"name": "validate-indexfiles",
				"description": "Validate index file sizes",
				"usage": "validate-indexfiles",
				"arguments": []map[string]string{},
			},
			{
				"name": "repair",
				"description": "Repair etcd data",
				"usage": "repair [subcommand]",
				"arguments": []map[string]string{},
				"subcommands": []string{
					"add_index_params",
					"channel",
					"channel-watch",
					"check_qn_collection_leak",
					"checkpoint",
					"diskann_index_params",
					"empty-segment",
					"index_metric_type",
					"legacy-collection-remnant",
					"manual-compaction",
					"segment",
				},
			},
		},
	}

	c.JSON(http.StatusOK, commands)
}

func (app *WebApp) handleDisconnect(c *gin.Context) {
	session := c.MustGet("session").(*Session)

	app.mu.Lock()
	defer app.mu.Unlock()

	if s, ok := app.sessions[session.id]; ok {
		// Properly close the state if it exists
		if s.state != nil {
			s.state.Close()
		}
		delete(app.sessions, session.id)
		fmt.Printf("Session %s disconnected\n", session.id)
	}

	// Expire cookie on client
	c.SetCookie("session_id", "", -1, "/", "", false, true)

	c.JSON(http.StatusOK, gin.H{"success": true})
}

 