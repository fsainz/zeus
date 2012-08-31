package zeusmaster

import (
	"strconv"
	"math/rand"
	"strings"
	"os"
	"os/exec"
	"fmt"
	"net"

	usock "github.com/burke/zeus/go/unixsocket"
	slog "github.com/burke/zeus/go/shinylog"
)

type SlaveMonitor struct {
	tree *ProcessTree
}

func StartSlaveMonitor(tree *ProcessTree, local *net.UnixConn, remote *os.File, quit chan bool) {
	monitor := &SlaveMonitor{tree}

	// We just want this unix socket to be a channel so we can select on it...
	registeringFds := make(chan int, 3)
	go func() {
		for {
			fd, err := usock.ReadFileDescriptorFromUnixSocket(local)
			if err != nil {
				fmt.Println(err)
			}
			registeringFds <- fd
		}
	}()

	go monitor.startInitialProcess(remote)

	for {
		select {
		case <- quit:
			quit <- true
			monitor.cleanupChildren()
			return
		case fd := <- registeringFds:
			go monitor.slaveDidBeginRegistration(fd)
		case name := <- monitor.tree.Booted:
			go monitor.slaveDidBoot(name)
		case name := <- monitor.tree.Dead:
			go monitor.slaveDidDie(name)
		}
	}
}

func (mon *SlaveMonitor) cleanupChildren() {
	mon.tree.Root.Kill(mon.tree)
}

func (mon *SlaveMonitor) slaveDidBoot(slaveName string) {
	bootedSlave := mon.tree.FindSlaveByName(slaveName)
	slog.SlaveBooted(bootedSlave.Name)
	for _, slave := range bootedSlave.Slaves {
		go mon.bootSlave(slave)
	}
}

func (mon *SlaveMonitor) slaveDidDie(slaveName string) {
	deadSlave := mon.tree.FindSlaveByName(slaveName)
	mon.bootSlave(deadSlave)
}


func (mon *SlaveMonitor) bootSlave(slave *SlaveNode) {
	slave.Parent.WaitUntilBooted()
	msg := CreateSpawnSlaveMessage(slave.Name)
	slave.Parent.Socket.Write([]byte(msg))
}

func (mon *SlaveMonitor) startInitialProcess(sock *os.File) {
	command := mon.tree.ExecCommand
	parts := strings.Split(command, " ")
	executable := parts[0]
	args := parts[1:]
	cmd := exec.Command(executable, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("ZEUS_MASTER_FD=%d", sock.Fd()))
	cmd.ExtraFiles = []*os.File{sock}

	// We want to let this process run "forever", but it will eventually
	// die... either on program termination or when its dependencies change
	// and we kill it.
	output, err := cmd.CombinedOutput()
	if err != nil && string(err.Error()[:11]) != "exit status" {
		ErrorConfigCommandCouldntStart(err.Error())
	} else {
		ErrorConfigCommandCrashed(string(output))
	}
}

func (mon *SlaveMonitor) slaveDidBeginRegistration(fd int) {
	// Having just started the process, we expect an IO, which we convert to a UNIX domain socket
	fileName := strconv.Itoa(rand.Int())
	slaveFile := usock.FdToFile(fd, fileName)
	slaveSocket, err := usock.MakeUnixSocket(slaveFile)
	if err != nil {
		fmt.Println(err)
	}
	if err = slaveSocket.SetReadBuffer(262144) ; err != nil {
		fmt.Println(err)
	}
	if err = slaveSocket.SetWriteBuffer(262144) ; err != nil {
		fmt.Println(err)
	}

	go mon.handleSlaveRegistration(slaveSocket)
}

func (mon *SlaveMonitor) handleSlaveRegistration(slaveSocket *net.UnixConn) {
	// We now expect the slave to use this fd they send us to send a Pid&Identifier Message
	msg, _, err := usock.ReadFromUnixSocket(slaveSocket)
	if err != nil {
		fmt.Println(err)
	}
	pid, identifier, err := ParsePidMessage(msg)

	node := mon.tree.FindSlaveByName(identifier)
	if node == nil {
		panic("Unknown identifier")
	}

	// TODO: We actually don't really want to prevent killing this
	// process while it's booting up.
	node.mu.Lock()
	defer node.mu.Unlock()

	node.Pid = pid

	if err != nil {
		fmt.Println(err)
	}

	// The slave will execute its action and respond with a status...
	msg, _, err = usock.ReadFromUnixSocket(slaveSocket)
	if err != nil {
		fmt.Println(err)
	}
	msg, err = ParseActionResponseMessage(msg)
	if err != nil {
		fmt.Println(err)
	}
	if msg == "OK" {
		node.Socket = slaveSocket
	} else {
		node.RegisterError(msg)
	}
	node.SignalBooted()
	mon.tree.Booted <- identifier
}
