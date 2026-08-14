package etcdraft

import "fmt"

// adapterLogger keeps normal upstream protocol chatter out of artifact and
// CLI output while preserving fatal and panic paths as process-visible bugs.
type adapterLogger struct{}

func (adapterLogger) Debug(...any)                        {}
func (adapterLogger) Debugf(string, ...any)               {}
func (adapterLogger) Info(...any)                         {}
func (adapterLogger) Infof(string, ...any)                {}
func (adapterLogger) Warning(...any)                      {}
func (adapterLogger) Warningf(string, ...any)             {}
func (adapterLogger) Error(...any)                        {}
func (adapterLogger) Errorf(string, ...any)               {}
func (adapterLogger) Fatal(values ...any)                 { panic(fmt.Sprint(values...)) }
func (adapterLogger) Fatalf(format string, values ...any) { panic(fmt.Sprintf(format, values...)) }
func (adapterLogger) Panic(values ...any)                 { panic(fmt.Sprint(values...)) }
func (adapterLogger) Panicf(format string, values ...any) { panic(fmt.Sprintf(format, values...)) }
