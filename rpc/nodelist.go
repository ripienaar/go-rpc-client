package rpc

import "sync"

type NodeList struct {
	sync.RWMutex
	hosts map[string]struct{}
}

func NewNodeList() *NodeList {
	return &NodeList{
		hosts: make(map[string]struct{}),
	}
}

func (n *NodeList) AddHosts(hosts []string) {
	n.Lock()
	defer n.Unlock()

	for _, h := range hosts {
		n.hosts[h] = struct{}{}
	}
}

func (n *NodeList) Clear() {
	n.Lock()
	defer n.Unlock()

	n.hosts = make(map[string]struct{})
}

func (n *NodeList) AddHost(host string) {
	n.Lock()
	defer n.Unlock()

	n.hosts[host] = struct{}{}
}

func (n *NodeList) Count() int {
	n.RLock()
	defer n.RUnlock()

	return len(n.hosts)
}

func (n *NodeList) Hosts() []string {
	n.RLock()
	defer n.RUnlock()

	result := []string{}

	for k := range n.hosts {
		result = append(result, k)
	}

	return result
}

func (n *NodeList) DeleteIfKnown(host string) bool {
	n.Lock()
	defer n.Unlock()

	_, found := n.hosts[host]
	if found {
		delete(n.hosts, host)
	}

	return found
}

func (n *NodeList) Have(host string) bool {
	n.RLock()
	defer n.RUnlock()

	_, ok := n.hosts[host]

	return ok
}

func (n *NodeList) HaveAny(hosts []string) bool {
	n.RLock()
	defer n.RUnlock()

	for _, host := range hosts {
		_, ok := n.hosts[host]
		if ok {
			return true
		}
	}

	return false
}
