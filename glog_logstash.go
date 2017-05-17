package glog

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type logstashMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// handleLogstashMessages sends logs to logstash.
func (l *loggingT) handleLogstashMessages() {
	var conn net.Conn

	initialDelay := 500.0 * time.Millisecond
	maxDelay := 90.0 * time.Second
	delay := initialDelay
	conn, _ = net.DialTimeout("tcp", l.logstashURL, 1*time.Second)
	for {
		select {
		case _ = <-l.logstashStop:
			conn.Close()
			return
		case data := <-l.logstashChan:
			lm := logstashMessage{}
			lm.Type = l.logstashType
			lm.Message = strings.TrimSpace(data)
			packet, err := json.Marshal(lm)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: Failed to marshall logstashMessage.\n",  msgPrefix())
				continue
			} else if conn != nil {
				_, err := fmt.Fprintln(conn, string(packet))
				if err != nil {
					conn = nil
					//fmt.Fprintf(os.Stderr, "%s: Failed to write to logstash server; err=%s\n", msgPrefix(), err)
				} else {
					// reset the delay once we were able to write something to logstash
					delay = initialDelay
					// fmt.Fprintf(os.Stderr, "%s: write logstashMessage complete.\n",  msgPrefix())
				}
			} else {
				// There is no connection, so the log line is dropped.
				// Might be nice to add a buffer here so that we can ship
				// logs after the connection is up.
			}
		default:
			time.Sleep(1 * time.Second)
		}

		if conn == nil {
			delay *= 2.0
			if delay > maxDelay {
				delay = maxDelay
			}
			// fmt.Fprintf(os.Stderr, "%s: no connection; sleeping %2.2f\n",  msgPrefix(), delay.Seconds())
			time.Sleep(delay)

			// fmt.Fprintf(os.Stderr, "%s: Trying to connect to logstash server %s\n", msgPrefix(), l.logstashURL)
			var err error
			conn, err = net.DialTimeout("tcp", l.logstashURL, 1*time.Second)
			if err != nil {
				conn = nil
				//fmt.Fprintf(os.Stderr, "%s: Failed to connect to logstash server; err=%s\n", msgPrefix(), err)
			}
		}
	}
}

// Message prefix for direct writes to stderr
func msgPrefix() string {
	return fmt.Sprintf("glog: %s: ", time.Now().Format("15:04:05.00000"))
}

// StartLogstash creates the logstash channel and kicks off handleLogstashMessages.
func (l *loggingT) startLogstash() {
	l.logstashChan = make(chan string, 100)
	go l.handleLogstashMessages()
}

// StopLogstash signals handleLogstashMessages to exit.
func (l *loggingT) StopLogstash() {
	l.logstashStop <- true
}
