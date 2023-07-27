package states

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/gosuri/uilive"
	"github.com/milvus-io/birdwatcher/eventlog"
	"go.uber.org/atomic"
)

type FrameScreen struct {
	display   *uilive.Writer
	lastPrint atomic.Int64
	lines     []io.Writer
	mut       sync.Mutex
}

func NewFrameScreen(lines int, display *uilive.Writer) *FrameScreen {
	if lines <= 0 {
		lines = 1
	}
	ws := make([]io.Writer, lines)
	ws[0] = display
	for i := 1; i < lines; i++ {
		ws[i] = display.Newline()
	}

	return &FrameScreen{
		display: display,
		lines:   ws,
	}
}

var (
	colorPending = color.New(color.FgYellow)
	colorReady   = color.New(color.FgGreen)

	levelColor = map[eventlog.Level]*color.Color{
		eventlog.Level_Debug: color.New(color.FgGreen),
		eventlog.Level_Info:  color.New(color.FgBlue),
		eventlog.Level_Warn:  color.New(color.FgYellow),
		eventlog.Level_Error: color.New(color.FgRed),
	}
)

func (s *FrameScreen) printEvent(evt *eventlog.Event) {
	s.mut.Lock()
	defer s.mut.Unlock()

	lvl := evt.GetLevel()
	fmt.Printf("[%s][%s]%s\n", time.Unix(0, evt.GetTs()).Format("01/02 15:04:05"), levelColor[lvl].Sprint(lvl.String()), string(evt.Data))
}

func (s *FrameScreen) printEvents(display *uilive.Writer, m *sync.Map, events []*eventlog.Event) {
	s.mut.Lock()
	defer s.mut.Unlock()
	t := time.Now()
	last := time.Unix(0, s.lastPrint.Load())
	if t.Sub(last) < time.Millisecond*50 {
		return
	}

	s.lastPrint.Store(t.UnixNano())
	_, rcOk := m.Load("rootcoord")
	rctext := colorPending.Sprint("Connecting")
	if rcOk {
		rctext = colorReady.Sprint("  Ready  ")
	}
	_, qcOk := m.Load("querycoord")
	qctext := colorPending.Sprintf("Connecting")
	if qcOk {
		qctext = colorReady.Sprintf("  Ready  ")
	}
	fmt.Fprintf(display, fmt.Sprintf("RootCoord[%s] QueryCoord[%s]\n", rctext, qctext))

	start := 0
	if len(events) > 10 {
		start = len(events) - 10
	}

	for i := start; i < len(events); i++ {
		evt := events[i]
		lvl := evt.GetLevel()
		fmt.Fprintf(s.lines[i+1], fmt.Sprintf("[%s][%s]%s\n", time.Unix(0, evt.GetTs()).Format("01/02 15:04:05"), levelColor[lvl].Sprint(lvl.String()), string(evt.Data)))
	}
}
