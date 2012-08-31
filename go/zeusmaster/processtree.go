package zeusmaster

import (
	"net"
	"sync"
	"syscall"

	usock "github.com/burke/zeus/go/unixsocket"
	slog "github.com/burke/zeus/go/shinylog"
)

type ProcessTree struct {
	Root *SlaveNode
	ExecCommand string
	SlavesByName map[string]*SlaveNode
	CommandsByName map[string]*CommandNode
	Booted chan string
	Dead   chan string
}

type ProcessTreeNode struct {
	mu sync.RWMutex
	Parent *SlaveNode
	Name string
}

type SlaveNode struct {
	ProcessTreeNode
	Socket *net.UnixConn
	Pid int
	Error string
	bootWait sync.RWMutex
	Slaves []*SlaveNode
	Commands []*CommandNode
	Features map[string]bool
}

type CommandNode struct {
	ProcessTreeNode
	booting sync.RWMutex
	Aliases []string
}

func (node *SlaveNode) WaitUntilBooted() {
	node.bootWait.RLock()
	node.bootWait.RUnlock()
}

func (node *SlaveNode) SignalBooted() {
	node.bootWait.Unlock()
	go node.listenForFeatures()
}

func (node *SlaveNode) SignalUnbooted() {
	node.bootWait.Lock()
}

func (tree *ProcessTree) NewCommandNode(name string, aliases []string, parent *SlaveNode) *CommandNode {
	x := &CommandNode{}
	x.Parent = parent
	x.Name = name
	x.Aliases = aliases
	tree.CommandsByName[name] = x
	return x
}

func (tree *ProcessTree) NewSlaveNode(name string, parent *SlaveNode) *SlaveNode {
	x := &SlaveNode{}
	x.Parent = parent
	x.SignalUnbooted()
	x.Name = name
	x.Features = make(map[string]bool)
	tree.SlavesByName[name] = x
	return x
}

func (tree *ProcessTree) FindSlaveByName(name string) *SlaveNode {
	if name == "" {
		return tree.Root
	}
	return tree.SlavesByName[name]
}

func (tree *ProcessTree) FindCommandByName(name string) *CommandNode {
	return tree.CommandsByName[name]
}

func (node *SlaveNode) RegisterError(msg string) {
	node.Error = msg
	for _, slave := range node.Slaves {
		slave.RegisterError(msg)
	}
}

func (node *SlaveNode) Wipe() {
	node.Pid = 0
	node.Socket = nil
	node.Error = ""
}

func (tree *ProcessTree) AllCommandsAndAliases() []string {
	var values []string
	for name, command := range tree.CommandsByName {
		values = append(values, name)
		for _, alias := range command.Aliases {
			values = append(values, alias)
		}
	}
	return values
}

func (node *SlaveNode) listenForFeatures() {
	sock := node.Socket
	for {
		msg, _, err := usock.ReadFromUnixSocket(sock)
		if err != nil {
			println(err.Error())
			return
		}
		file, err := ParseFeatureMessage(msg)
		if err != nil {
			println("listenForFeatures(" + node.Name + "): " + err.Error())
		}
		node.addFeature(file)
	}
}

func (node *SlaveNode) addFeature(file string) {
	node.Features[file] = true
	AddFile(file)
}

func (node *SlaveNode) Kill(tree *ProcessTree) {
	node.mu.Lock()
	defer node.mu.Unlock()

	pid := node.Pid
	if pid > 0 {
		err := syscall.Kill(pid, 9) // error implies already dead -- no worries.
		if err != nil {
			slog.SlaveKilled(node.Name)
		} else {
			slog.SlaveDied(node.Name)
		}
		node.SignalUnbooted()
		tree.Dead <- node.Name
	}
	node.Wipe()

	for _, s := range node.Slaves {
		go s.Kill(tree)
	}
}
func (tree *ProcessTree) KillNodesWithFeature(file string) {
	tree.Root.killNodesWithFeature(tree, file)
}

func (node *SlaveNode) killNodesWithFeature(tree *ProcessTree, file string) {
	if node.Features[file] {
		node.Kill(tree, )
	} else {
		for _, s := range node.Slaves {
			s.killNodesWithFeature(tree, file)
		}
	}
}
