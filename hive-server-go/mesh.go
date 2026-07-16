package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

type PeerInfo struct {
	ServerID        string  `json:"server_id"`
	Endpoint        string  `json:"endpoint"`
	MaxConcurrent   int     `json:"max_concurrent"`
	OllamaModel     string  `json:"ollama_model"`
	LastSeen        float64 `json:"last_seen"`
	PendingJobs     int     `json:"pending_jobs"`
	RunningJobs     int     `json:"running_jobs"`
	Clients         int     `json:"clients"`
	AvailableCap    int     `json:"available_capacity"`
	Load            float64 `json:"load"`
	Alive           bool    `json:"alive"`
}

type MeshDiscovery struct {
	serverID      string
	serverPort    int
	discoveryPort int
	maxConcurrent int
	ollamaModel   string
	announceAddr  string // resolved LAN IP or env override

	mu    sync.RWMutex
	peers map[string]*PeerInfo

	httpClient     *http.Client
	getCapacity    func() int
	getQueueStatus func() map[string]interface{}
	getClients     func() int

	stopCh chan struct{}
}

func NewMeshDiscovery(serverID string, serverPort, discoveryPort, maxConcurrent int, ollamaModel string) *MeshDiscovery {
	announceAddr := resolveAnnounceAddress(serverPort)
	logInfo("Mesh announce address: %s", announceAddr)
	return &MeshDiscovery{
		serverID:      serverID,
		serverPort:    serverPort,
		discoveryPort: discoveryPort,
		maxConcurrent: maxConcurrent,
		ollamaModel:   ollamaModel,
		announceAddr:  announceAddr,
		peers:         make(map[string]*PeerInfo),
		httpClient:    &http.Client{Timeout: 5 * time.Second},
		stopCh:        make(chan struct{}),
	}
}

// resolveAnnounceAddress determines the IP this server advertises to peers.
// Priority: MESH_ANNOUNCE_ADDRESS env > auto-detect LAN IP > localhost fallback
func resolveAnnounceAddress(serverPort int) string {
	// 1. Explicit override via env
	if addr := os.Getenv("MESH_ANNOUNCE_ADDRESS"); addr != "" {
		if u, err := url.Parse(addr); err == nil && u.Host != "" {
			return u.Host
		}
		return addr
	}

	// 2. Auto-detect LAN IP
	if ip := detectLocalIP(); ip != "" {
		return fmt.Sprintf("http://%s:%d", ip, serverPort)
	}

	// 3. Fallback
	return fmt.Sprintf("http://localhost:%d", serverPort)
}

// detectLocalIP finds the first non-loopback, non-container IPv4 address
// by enumerating network interfaces. Works on macOS, Linux, and Docker.
func detectLocalIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range ifaces {
		// Skip down interfaces
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		// Skip loopback
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// Skip common virtual/container interfaces
		name := iface.Name
		if name == "" {
			continue
		}
		// Skip Docker, br-, veth, virbr, tun, wg, utun prefixes
		skipPrefixes := []string{"docker", "br-", "veth", "virbr", "tun", "wg", "utun", "lo", "vmnet", "utun"}
		skip := false
		for _, p := range skipPrefixes {
			if len(name) >= len(p) && name[:len(p)] == p {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			// Only IPv4
			if ip.To4() == nil {
				continue
			}
			// Skip link-local
			if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			return ip.String()
		}
	}
	return ""
}

func (m *MeshDiscovery) Start(capacityFn func() int, queueFn func() map[string]interface{}, clientsFn func() int) {
	m.getCapacity = capacityFn
	m.getQueueStatus = queueFn
	m.getClients = clientsFn

	seedPeers := getSeedPeers()
	for _, addr := range seedPeers {
		m.probeSeed(addr)
	}

	go m.broadcastLoop()
	go m.listenerLoop()
	go m.cleanupLoop()
	go m.seedProbeLoop(seedPeers)
	logInfo("Mesh discovery started on port %d, announcing as %s", m.discoveryPort, m.announceAddr)
}

func (m *MeshDiscovery) Stop() {
	close(m.stopCh)
	logInfo("Mesh discovery stopped")
}

func (m *MeshDiscovery) probeSeed(addr string) bool {
	baseURL := fmt.Sprintf("http://%s", addr)
	resp, err := m.httpClient.Get(baseURL + "/api/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return false
	}
	serverID, _ := data["server_id"].(string)
	if serverID == "" || serverID == m.serverID {
		return false
	}

	var maxC int
	if q, ok := data["queue"].(map[string]interface{}); ok {
		if mc, ok := q["max_concurrent"].(float64); ok {
			maxC = int(mc)
		}
	}
	ollamaModel, _ := data["ollama_model"].(string)

	m.mu.Lock()
	if _, exists := m.peers[serverID]; !exists {
		logInfo("Discovered seed peer: %s at %s", serverID, addr)
	}
	m.peers[serverID] = &PeerInfo{
		ServerID:      serverID,
		Endpoint:      baseURL,
		MaxConcurrent: maxC,
		OllamaModel:   ollamaModel,
		LastSeen:      now(),
	}
	m.mu.Unlock()

	m.introduceSelf(baseURL)
	m.exchangePeers(baseURL)
	return true
}

func (m *MeshDiscovery) introduceSelf(peerBaseURL string) {
	body, _ := json.Marshal(map[string]interface{}{
		"server_id":      m.serverID,
		"endpoint":       m.announceAddr,
		"max_concurrent": m.maxConcurrent,
		"ollama_model":   m.ollamaModel,
	})
	resp, err := m.httpClient.Post(peerBaseURL+"/api/peers/introduce", "application/json", bytes.NewReader(body))
	if err != nil {
		logWarn("Failed to introduce self to %s: %v", peerBaseURL, err)
		return
	}
	defer resp.Body.Close()
}

func (m *MeshDiscovery) exchangePeers(baseURL string) {
	resp, err := m.httpClient.Get(baseURL + "/api/peers")
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var data struct {
		Peers []PeerInfo `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return
	}
	m.mu.Lock()
	for _, p := range data.Peers {
		if p.ServerID != m.serverID {
			if _, exists := m.peers[p.ServerID]; !exists {
				logInfo("Learned peer from %s: %s at %s", baseURL, p.ServerID, p.Endpoint)
				peer := p
				peer.LastSeen = now()
				m.peers[p.ServerID] = &peer
			}
		}
	}
	m.mu.Unlock()
}

func (m *MeshDiscovery) broadcastLoop() {
	for {
		select {
		case <-m.stopCh:
			return
		default:
			m.sendBeacon()
			time.Sleep(10 * time.Second)
		}
	}
}

func (m *MeshDiscovery) sendBeacon() {
	beacon := map[string]interface{}{
		"server_id":      m.serverID,
		"port":           m.serverPort,
		"max_concurrent": m.maxConcurrent,
		"ollama_model":   m.ollamaModel,
	}
	if m.getQueueStatus != nil {
		q := m.getQueueStatus()
		beacon["pending_jobs"] = q["pending"]
		beacon["running_jobs"] = q["running"]
	}
	if m.getCapacity != nil {
		beacon["available_capacity"] = m.getCapacity()
	}
	if m.getClients != nil {
		beacon["clients"] = m.getClients()
	}

	data, _ := json.Marshal(beacon)

	// Broadcast on all interfaces using subnet-directed broadcast
	m.broadcastOnAllInterfaces(data)
}

// broadcastOnAllInterfaces sends the beacon to every IPv4 broadcast address
// on every active interface. This covers the case where machines are on
// different subnets (e.g. wired vs wifi, or Docker bridges).
func (m *MeshDiscovery) broadcastOnAllInterfaces(data []byte) {
	ifaces, err := net.Interfaces()
	if err != nil {
		// Fallback to limited broadcast
		m.sendBeaconTo(net.IPv4bcast, data)
		return
	}

	sent := false
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.To4() == nil || ip.IsLoopback() {
				continue
			}
			bcast := broadcastAddr(ip, ip.DefaultMask())
			if bcast != "" {
				m.sendBeaconTo(net.ParseIP(bcast), data)
				sent = true
			}
		}
	}

	// Also send on limited broadcast as fallback
	if !sent {
		m.sendBeaconTo(net.IPv4bcast, data)
	}
}

// broadcastAddr computes the subnet broadcast address from an IP and mask
func broadcastAddr(ip net.IP, mask net.IPMask) string {
	ip4 := ip.To4()
	if ip4 == nil {
		return ""
	}
	// mask is big-endian, broadcast = ip | ~mask
	bcast := make(net.IP, 4)
	for i := 0; i < 4; i++ {
		bcast[i] = ip4[i] | ^mask[i]
	}
	return bcast.String()
}

func (m *MeshDiscovery) sendBeaconTo(bcastIP net.IP, data []byte) {
	addr := &net.UDPAddr{IP: bcastIP, Port: m.discoveryPort}
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write(data)
}

func (m *MeshDiscovery) listenerLoop() {
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: m.discoveryPort}
	conn, err := net.ListenUDP("udp4", addr)
	if err != nil {
		logError("Mesh listener failed: %v", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 4096)
	for {
		select {
		case <-m.stopCh:
			return
		default:
			conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, remoteAddr, err := conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			m.handleBeacon(buf[:n], remoteAddr)
		}
	}
}

func (m *MeshDiscovery) handleBeacon(data []byte, addr *net.UDPAddr) {
	var beacon map[string]interface{}
	if err := json.Unmarshal(data, &beacon); err != nil {
		return
	}
	peerID, _ := beacon["server_id"].(string)
	if peerID == "" || peerID == m.serverID {
		return
	}

	port, _ := beacon["port"].(float64)
	endpoint := fmt.Sprintf("http://%s:%d", addr.IP.String(), int(port))
	t := now()

	m.mu.Lock()
	if existing, ok := m.peers[peerID]; ok {
		existing.Endpoint = endpoint
		existing.LastSeen = t
		if mc, ok := beacon["max_concurrent"].(float64); ok {
			existing.MaxConcurrent = int(mc)
		}
		if mo, ok := beacon["ollama_model"].(string); ok {
			existing.OllamaModel = mo
		}
		if pj, ok := beacon["pending_jobs"].(float64); ok {
			existing.PendingJobs = int(pj)
		}
		if rj, ok := beacon["running_jobs"].(float64); ok {
			existing.RunningJobs = int(rj)
		}
		if c, ok := beacon["clients"].(float64); ok {
			existing.Clients = int(c)
		}
		// available_capacity from beacon is more accurate than computed from running_jobs
		if ac, ok := beacon["available_capacity"].(float64); ok {
			existing.AvailableCap = int(ac)
		}
	} else {
		peer := &PeerInfo{
			ServerID:      peerID,
			Endpoint:      endpoint,
			MaxConcurrent: int(getFloat(beacon, "max_concurrent", 2)),
			OllamaModel:   getStr(beacon, "ollama_model"),
			LastSeen:      t,
			PendingJobs:   int(getFloat(beacon, "pending_jobs", 0)),
			RunningJobs:   int(getFloat(beacon, "running_jobs", 0)),
			Clients:       int(getFloat(beacon, "clients", 0)),
			AvailableCap:  int(getFloat(beacon, "available_capacity", 0)),
		}
		m.peers[peerID] = peer
		logInfo("Discovered peer: %s at %s", peerID, endpoint)
	}
	m.mu.Unlock()
}

func (m *MeshDiscovery) cleanupLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.mu.Lock()
			cutoff := now() - 30
			for id, p := range m.peers {
				if p.LastSeen < cutoff {
					logInfo("Peer timed out: %s", id)
					delete(m.peers, id)
				}
			}
			m.mu.Unlock()
		}
	}
}

func (m *MeshDiscovery) seedProbeLoop(seedPeers []string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			for _, addr := range seedPeers {
				m.probeSeed(addr)
			}
		}
	}
}

func (m *MeshDiscovery) GetAlivePeers() []*PeerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var alive []*PeerInfo
	for _, p := range m.peers {
		if now()-p.LastSeen < 30 {
			p.Alive = true
			p.AvailableCap = max(0, p.MaxConcurrent-p.RunningJobs)
			p.Load = calcLoad(p.RunningJobs, p.PendingJobs, p.MaxConcurrent)
			alive = append(alive, p)
		}
	}
	return alive
}

func (m *MeshDiscovery) GetBestPeer() *PeerInfo {
	alive := m.GetAlivePeers()
	if len(alive) == 0 {
		return nil
	}
	var best *PeerInfo
	for _, p := range alive {
		if p.AvailableCap > 0 {
			if best == nil || (p.Load < best.Load) || (p.Load == best.Load && p.AvailableCap > best.AvailableCap) {
				best = p
			}
		}
	}
	return best
}

func (m *MeshDiscovery) RegisterPeer(serverID, endpoint string, maxConcurrent int, ollamaModel string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers[serverID] = &PeerInfo{
		ServerID:      serverID,
		Endpoint:      endpoint,
		MaxConcurrent: maxConcurrent,
		OllamaModel:   ollamaModel,
		LastSeen:      now(),
	}
	logInfo("Manually registered peer: %s at %s", serverID, endpoint)
}

func (m *MeshDiscovery) UnregisterPeer(serverID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.peers, serverID)
	logInfo("Removed peer: %s", serverID)
}

func (m *MeshDiscovery) Rescan(seedPeers []string) {
	for _, addr := range seedPeers {
		m.probeSeed(addr)
	}
	logInfo("Mesh re-scan complete")
}

func (m *MeshDiscovery) GetDiagnostics() map[string]interface{} {
	alive := m.GetAlivePeers()
	return map[string]interface{}{
		"enabled":        true,
		"server_id":      m.serverID,
		"discovery_port": m.discoveryPort,
		"server_port":    m.serverPort,
		"announce_addr":  m.announceAddr,
		"peers_total":    len(m.peers),
		"peers_alive":    len(alive),
		"seed_peers":     getSeedPeers(),
	}
}

func (m *MeshDiscovery) GetPeersList() []PeerInfo {
	alive := m.GetAlivePeers()
	list := make([]PeerInfo, len(alive))
	for i, p := range alive {
		list[i] = *p
	}
	return list
}

func getFloat(m map[string]interface{}, key string, def float64) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return def
}

func getStr(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func calcLoad(running, pending, maxC int) float64 {
	if maxC == 0 {
		return 1.0
	}
	load := float64(running+pending) / float64(maxC)
	if load > 1.0 {
		return 1.0
	}
	return load
}

func getSeedPeers() []string {
	peers := os.Getenv("MESH_SEED_PEERS")
	if peers == "" {
		return nil
	}
	var result []string
	for _, p := range splitAndTrim(peers, ",") {
		if p != "" {
			if u, err := url.Parse(p); err == nil && u.Host != "" {
				result = append(result, u.Host)
			} else {
				result = append(result, p)
			}
		}
	}
	return result
}
