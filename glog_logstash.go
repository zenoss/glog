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
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Loglevel  string    `json:"loglevel"`
	Timestamp time.Time `json:"@timestamp"`
}

var GlogDialer = net.Dial

// handleLogstashMessages sends logs to logstash.
func (l *loggingT) handleLogstashMessages() {
	var conn net.Conn
	dialer := time.Tick(1 * time.Second)
	var datachan chan string
	var sysexit bool
	for {
		select {
		case sysexit = <-l.logstashStop:
			close(l.logstashChan)
		case _ = <-dialer:
			var err error
			fmt.Fprintln(os.Stderr, "Trying to connect to logstash server...")
			if conn, err = GlogDialer("tcp", l.logstashURL); err != nil {
				conn = nil
			} else {
				dialer = nil
				datachan = l.logstashChan
				fmt.Fprintln(os.Stderr, "Connected to logstash server.")
			}
		case data, ok := <-datachan:
			if !ok {
				if sysexit {
					os.Exit(255)
				}
				return
			}
			lm := logstashMessage{}
			lm.Type = l.logstashType
			lm.Message = strings.TrimSpace(data)
			packet, err := json.Marshal(lm)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Failed to marshal logstashMessage.")
				continue
			}
			if _, err := fmt.Fprintln(conn, string(packet)); err != nil {
				fmt.Fprintln(os.Stderr, "Not connected to logstash server, attempting reconnect.")
				conn = nil
				dialer = time.Tick(1 * time.Second)
				continue
			}
		}
	}
}

// StartLogstash creates the logstash channel and kicks off handleLogstashMessages.
func (l *loggingT) startLogstash() {
	l.logstashChan = make(chan string, 100)
	go l.handleLogstashMessages()
}

// StopLogstash signals handleLogstashMessages to exit.
func (l *loggingT) StopLogstash(sysexit bool) {
	l.logstashStop <- sysexit
	if sysexit {
		// wait for loop to exit, if not caller will os.Exit
		time.Sleep(time.Second * 10)
	}
}
