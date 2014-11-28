// +build !linux

package rqw

import "syscall"

func sysProcAttr() *syscall.SysProcAttr { return nil }
