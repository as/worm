package worm

import (
	"time"

	"github.com/as/event"
)

// Coalescer is a WORM logger that coalesces logs written to
// it until the deadband expires. When the deadband expires,
// coalesced logs are flushed to the underlying logger during
// the next call to Write().
//
// When a log is written to the underlying WORM, the
// deadband period is reset to its original value given
// to NewCoalescer.
//
//
type Coalescer struct {
	Logger

	last event.Record

	// period during which coalesced writes can be
	// buffered without flush to the underlying logger
	deadband time.Duration

	timer *time.Timer
}

// NewCoalescer wraps the given logger and returns a coalescer
func NewCoalescer(lg Logger, deadband time.Duration) *Coalescer {
	return &Coalescer{
		Logger:   lg,
		last:     nil,
		deadband: deadband,
		timer:    time.NewTimer(deadband),
	}
}

// ReadAt reads and returns log record n
func (l *Coalescer) ReadAt(n int64) (event.Record, error) {
	return l.Logger.ReadAt(n)
}

// Write writes v to the tail of the log
func (l *Coalescer) Write(v event.Record) (err error) {
	if l.last == nil {
		l.last = v
		return nil
	}
	select {
	case <-l.timer.C:
		defer l.timer.Reset(l.deadband)
		co := l.last.Coalesce(v)
		l.Logger.Write(l.last)
		if co {
			l.last = nil
		} else {
			l.last = v
		}
		return nil
	default:
		if l.last.Coalesce(v) {
			if !l.timer.Stop() {
				<-l.timer.C
			}
			l.timer.Reset(l.deadband)
			return nil
		}
		l.Logger.Write(l.last)
		l.last = v
		return nil
	}
}

// Flush flushes the last unwritten log to the underlying logger
func (l *Coalescer) Flush() error {
	return l.Logger.Write(l.last)
}
