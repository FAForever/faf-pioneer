package webrtc

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/gpgnet"
	"faf-pioneer/icebreaker"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
	"net"
	"sync"
	"time"
	"fmt"
	"strconv"
	"math"
	"strings"
)

var allTurnServersUrls 	[]string
var turnsNameById = make(map[int]string)

type onPeerCandidatesGatheredCallback = func(*webrtc.SessionDescription, []webrtc.ICECandidate)

const (
	maxLobbyPeers = 30
)
const (
	// peerDisconnectedTimeout is a duration without network activity before an Agent is considered disconnected.
	// Default is 5 Seconds.
	peerDisconnectedTimeout = time.Second * 10
	// peerFailedTimeout is a duration without network activity before an Agent is considered
	// failed after disconnected.
	// Default is 25 Seconds.
	peerFailedTimeout = time.Second * 30
	// peerKeepAliveInterval is an interval how often the ICE Agent sends extra traffic if there is no activity,
	// if media is flowing no traffic will be sent.
	peerKeepAliveInterval = time.Second * 5
	// peerReconnectionInterval is an interval how often we will be trying to reconnect after failed reconnection
	// attempt when Peer goes to Failed/Closed state.
	peerReconnectionInterval = time.Second * 20
)

type PeerHandler interface {
	AddPeerIfMissing(playerId uint) PeerMeta
	RemovePeer(playerId uint)
	GetPeerById(playerId uint) (*Peer, bool)
}

type PeerManager struct {
	ctx                  context.Context
	localUserId          uint
	gameId               uint64
	peersMu              sync.Mutex
	peers                map[uint]*Peer
	icebreakerClient     *icebreaker.Client
	icebreakerEvents     <-chan icebreaker.EventMessage
	turnServer           []webrtc.ICEServer
	gameUdpPort          uint
	forceTurnRelay       bool
	reconnectionRequests chan uint
	gpgNetToGameChannel  chan<- gpgnet.Message
	disableRecBtnPeers   []string
}

func NewPeerManager(
	ctx context.Context,
	icebreakerClient *icebreaker.Client,
	launcherInfo *launcher.Info,
	sessionInfo *icebreaker.SessionGameResponse,
	gameUdpPort uint,
	turnServer []webrtc.ICEServer,
	icebreakerEvents <-chan icebreaker.EventMessage,
	gpgNetToGameChannel chan<- gpgnet.Message,
) *PeerManager {
	peerManager := PeerManager{
		ctx:                  ctx,
		localUserId:          launcherInfo.UserId,
		gameId:               launcherInfo.GameId,
		peers:                make(map[uint]*Peer),
		icebreakerClient:     icebreakerClient,
		icebreakerEvents:     icebreakerEvents,
		turnServer:           turnServer,
		gameUdpPort:          gameUdpPort,
		forceTurnRelay:       true,
		reconnectionRequests: make(chan uint, maxLobbyPeers),
		gpgNetToGameChannel:  gpgNetToGameChannel,
	}

	peerManager.saveTurnsURLs()

	// Note:
	// Setting maxLobbyPeers for `reconnectionRequests` initial capacity does not bound its size:
	// maps grow to accommodate the number of items stored in them.

	return &peerManager
}

func (p *PeerManager) IsTurnRelayForced() bool {
	return p.forceTurnRelay
}

func (p *PeerManager) GetGameUdpPort() uint {
	return p.gameUdpPort
}

func (p *PeerManager) saveTurnsURLs() {
	for i, _ := range(p.turnServer) {
		for _,url := range(p.turnServer[i].URLs) {
			_, ok := turnsNameById[i+1]
			if !ok {
				preparedName := " "
				index := strings.Index(url, ".com")

				if index != -1 {
					preparedName = url[:index]
					index = strings.LastIndex(preparedName, ".")
					if index != -1 {
						preparedName = preparedName[index+1:]
						if len(preparedName) > 6 {
							preparedName = preparedName[:6]
						}
					} else {
						preparedName = preparedName[len(preparedName) - 8:]
					}
				}
				turnsNameById[i+1] = preparedName
			}
			allTurnServersUrls = append(allTurnServersUrls, url)
		}
	}
}

func (p *PeerManager) Start() {
	go p.runReconnectionManagement()

	for {
		select {
		case msg, ok := <-p.icebreakerEvents:
			if !ok {
				return
			}

			p.handleIceBreakerEvent(msg)
		case <-p.ctx.Done():
			applog.Debug("Peer manager exited, context canceled")
			return
		}
	}
}

func (p *PeerManager) runReconnectionManagement() {
	for {
		select {
		case peerId := <-p.reconnectionRequests:
			p.handleReconnection(peerId, false)
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *PeerManager) handleReconnection(playerId uint, forceReconnect bool) {
	applog.Debug("Handling reconnection for peer", zap.Uint("playerId", playerId))

	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	if !ok || peer.IsDisabled() {
		p.peersMu.Unlock()
		return
	}
	p.peersMu.Unlock()

	if !forceReconnect && !peer.manualReconnIsActive && !peer.remoteManualRRequest{
		if peer.IsActive() || (peer.connection != nil &&
			peer.connection.ConnectionState() == webrtc.PeerConnectionStateConnecting) {
			applog.Info("Peer already active/connecting, skipping reconnection", zap.Uint("peer", playerId))
			return
		}
	}

	
	if !peer.manualReconnIsActive && !peer.remoteManualRRequest{
		applog.Info("Connecting to peer normally", zap.Uint("playerId", playerId))

		// These ids are used only by UI to show turn's names
		// There is no `url` field in ICECandidate object (only ip)
		// For manual reconn we know what turns will be used and save them
		// And here we have to reset ids as default turn list is in use and we can't say which one will be picked
		peer.specTurnIdLocal = 0
		peer.specTurnIdRemote = 0
		
		if err := peer.ConnectOnce(p.turnServer); err != nil {
			applog.Error("Peer normal connection failed", zap.Uint("peer", playerId), zap.Error(err))

			// Retry after `peerReconnectionInterval`.
			select {
			case <-p.ctx.Done():
			case <-time.After(peerReconnectionInterval):
				peer.reconnectMu.Lock()
				peer.reconnectionScheduled = false
				peer.reconnectMu.Unlock()
				p.scheduleReconnection(playerId)
			}
			return
		}
	} else {
		applog.Info("Connecting to peer with specific TURN", zap.Uint("playerId", playerId))
		if err := peer.ConnectOnce(peer.peerSpecificTurn); err != nil {
			applog.Error("Peer connection with specific TURN failed", zap.Uint("peer", playerId), zap.Error(err))
		}
		peer.reconnectMu.Lock()
		peer.manualReconnIsActive = false
		peer.remoteManualRRequest = false
		peer.reconnectMu.Unlock()
	}

	applog.Info("Peer connected successfully", zap.Uint("peer", playerId))
}

func (p *PeerManager) scheduleReconnection(playerId uint) {
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	p.peersMu.Unlock()
	if !ok || peer.IsDisabled() {
		return
	}

	peer.reconnectMu.Lock()
	if peer.reconnectionScheduled ||
		(peer.connection != nil &&
			peer.connection.ConnectionState() == webrtc.PeerConnectionStateConnecting) {
		peer.reconnectMu.Unlock()
		return
	}
	peer.reconnectionScheduled = true
	peer.reconnectMu.Unlock()

	select {
	case p.reconnectionRequests <- playerId:
		applog.Debug("Reconnect scheduled", zap.Uint("peer", playerId))
	default:
		peer.reconnectMu.Lock()
		peer.reconnectionScheduled = false
		peer.reconnectMu.Unlock()
		applog.Warn("Reconnect queue overflow", zap.Uint("peer", playerId))
	}
}

func (p *PeerManager) handleIceBreakerEvent(msg icebreaker.EventMessage) {
	switch event := msg.(type) {
	case *icebreaker.ConnectedMessage:
		applog.Info("Connecting to peer", zap.Any("event", event))
		p.addPeerIfMissing(event.SenderID)
	case *icebreaker.CandidatesMessage:
		// A dirty hook until we add a reconn message
		if len(event.Candidates) == 0 {
			applog.Info("Received ManualReconnectionMessage from peer: " + 
				fmt.Sprintf("%d", event.SenderID), zap.Any("event", event))
			p.handleRemoteManualReconnRequest(event.SenderID)
			return
		}

		applog.Info("Received CandidatesMessage", zap.Any("event", event))

		peer := p.peers[event.SenderID]
		if peer == nil {
			peer = p.addPeerIfMissing(event.SenderID)
			if peer == nil {
				applog.Error("Peer still nil after adding it as missing one")
				return
			}
		}

		if peer.connection == nil {
			applog.Warn("Peer connection is not initialized yet")
			go func() {
				time.Sleep(time.Millisecond * 500)
				p.handleIceBreakerEvent(msg)
			}()
			return
		}

		if peer.connection.ICEConnectionState() != webrtc.ICEConnectionStateConnected {
			err := peer.AddCandidates(event.Session, event.Candidates)
			if err != nil {
				applog.FromContext(peer.ctx).Error(
					"Could not add candidate to active peer connection",
					zap.Error(err),
				)
			}
		}
	case *icebreaker.PeerClosingMessage:
		applog.Info("Peer connection closed", zap.Any("event", event))

		p.RemovePeer(event.SenderID)

		applog.Info("Sending peer disconnected message to game from icebreaker peer closing message")
		p.gpgNetToGameChannel <- gpgnet.NewDisconnectFromPeerMessage(int32(event.SenderID))
	default:
		applog.Info("Received unknown event type", zap.Any("event", event))
	}
}

func (p *PeerManager) AddPeerIfMissing(playerId uint) PeerMeta {
	return p.addPeerIfMissing(playerId)
}

func (p *PeerManager) RemovePeer(playerId uint) {
	p.removePeer(playerId)
}

func (p *PeerManager) GetPeerById(playerId uint) (*Peer, bool) {
	p.peersMu.Lock()
	defer p.peersMu.Unlock()
	peer, ok := p.peers[playerId]
	return peer, ok
}

func (p *PeerManager) GetAllPeersStats() (map[string]webrtc.StatsReport, map[string]string, map[string][]int, []string) {
	p.peersMu.Lock()
	defer p.peersMu.Unlock()

	allPeersStats := make(map[string]webrtc.StatsReport)
	connectionStates := make(map[string]string)
	turnIds := make(map[string][]int)
	idsToDisable := p.disableRecBtnPeers
	p.disableRecBtnPeers = nil

	for id,peer := range(p.peers) {
		if peer == nil || peer.connection == nil{
			continue
		}

		idAsString := fmt.Sprintf("%d", id)
		allPeersStats[idAsString] = peer.connection.GetStats()
		connectionStates[idAsString] = peer.connection.ConnectionState().String()
		turnIds[idAsString] = []int{peer.specTurnIdLocal, peer.specTurnIdRemote}
	}

	return allPeersStats, connectionStates, turnIds, idsToDisable
}

func (p *PeerManager) GetAllPeerIds() []uint {
	ids := make([]uint, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}
	return ids
}

func (p *PeerManager) GetTurnURLs() ([]string, map[int]string, bool) {
	return allTurnServersUrls, turnsNameById, p.forceTurnRelay
}

func (p *PeerManager) removePeer(playerId uint) {
	peer := p.peers[playerId]
	if peer != nil {
		_ = peer.Close()
		delete(p.peers, playerId)
	}
}

func (p *PeerManager) addPeerIfMissing(playerId uint) *Peer {
	p.peersMu.Lock()
	// If peer exists, schedule reconnection if needed and return peer pointer instantly.
	if peer, ok := p.peers[playerId]; ok {
		p.peersMu.Unlock()
		if !peer.IsActive() && !peer.IsDisabled() {
			tSincePeerCreation := time.Now().Unix() - peer.creationTimeSeconds

			// Don't initiate reconnection on start (first 60 seconds)
			// as it breaks a normal attempt that will succeed in a few seconds
			// There is an additional connection watcher that works first 2 min -- DISABLED
			// Will need need proper fix later
			if tSincePeerCreation > 60 {
				applog.Info("Peer exists but is inactive, scheduling reconnection",
					zap.Uint("playerId", playerId),
				)


				p.scheduleReconnection(playerId)
			} else {
				applog.Info("Skipping reconnection since peer "+ fmt.Sprintf("%d", playerId) + 
				" was created " + strconv.FormatInt(tSincePeerCreation, 10) + " seconds ago.")
			}	
		} else if peer.IsActive() {
			applog.Info("Peer already exists and is active", zap.Uint("playerId", playerId))
		}

		return peer
	}

	// The smaller user id is always the offerer.
	isOfferer := p.localUserId < playerId
	peerUdpPort, err := util.GetFreeUdpPort()
	if err != nil {
		p.peersMu.Unlock()
		applog.Error("Failed to get UDP port for new peer", zap.Uint("playerId", playerId), zap.Error(err))
		return nil
	}

	applog.Info("Creating new peer", zap.Uint("playerId", playerId))
	newPeer, err := CreatePeer(
		p.ctx,
		isOfferer,
		playerId,
		p,
		peerUdpPort,
		p.gameUdpPort,
	)
	if err != nil {
		p.peersMu.Unlock()
		applog.FromContext(p.ctx).Error("Failed to create peer", zap.Error(err))
		return nil
	}

	newPeer.onStateChanged = p.onPeerStateChanged
	p.peers[playerId] = newPeer
	p.peersMu.Unlock()

	// Initiate connection in async manner instantly.
	go func() {
		p.reconnectPeer(playerId)
	}()

	applog.Debug("Peer successfully created", zap.Uint("playerId", playerId))
	
	// Initial connection watcher is temporary disabled
	// It takes quite some time for RU players to connect (up to 40-60 seconds)
	// We need to be very careful with any watchers or set high delays so it doesn't break a good atempt
	// Also players can always reconnect manually, so idk should we even launch some auto reconn on start
	
	// go func() {
	// 	p.initialConnectionWatcher(playerId)
	// }()

	return newPeer
}

func (p *PeerManager) reconnectPeer(playerId uint) {
	peer, ok := p.GetPeerById(playerId)
	if !ok || peer.IsDisabled() {
		return
	}

	applog.Info("Initial connecting to peer", zap.Uint("peer", playerId))
	if err := peer.ConnectOnce(p.turnServer); err != nil {
		applog.Error("Initial peer connection failed", zap.Uint("peer", playerId), zap.Error(err))
		// Reschedule connection attempt via queue.
		p.scheduleReconnection(playerId)
		return
	}
	applog.Info("Peer connected on initial attempt", zap.Uint("peer", playerId))
}

func (p *PeerManager) onPeerStateChanged(peer *Peer, state webrtc.PeerConnectionState) {
	applog.FromContext(peer.ctx).Info(
		"Peer connection state has changed",
		zap.String("state", state.String()),
	)

	switch state {
	case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
		if !peer.manualReconnIsActive {
			select {
			case <-peer.localAddrReady:
			default:
				peer.localAddrReadyOnce.Do(func() {
					close(peer.localAddrReady)
				})
			}

			applog.FromContext(peer.ctx).Info(
				"Peer connection failed or closed, scheduling immediate reconnection")

			// Do not uncomment this. Switching from relay to non relay doesn't work well and kinda bugged
			// I left it here as notification, so if someone want to try something similar in the future
			// he should do it carefully and with proper testing

			// if state == webrtc.PeerConnectionStateFailed && peer.forceTurnRelay {
			// 	applog.FromContext(peer.ctx).Info("Switching to fallback relay All policy")
			// 	peer.forceTurnRelay = false
			// }

			//peer.reconnectMu.Lock()
			//peer.reconnectionScheduled = false
			//peer.reconnectMu.Unlock()

			p.scheduleReconnection(peer.PeerId())
			break
		} else {
			applog.Debug("Peer is already reconnecting manually. Skipping 'failed or closed' case")
		}
	case webrtc.PeerConnectionStateDisconnected:
		// WebRTC documentation saying:
		// The ICE Agent has determined that connectivity is currently lost for this RTCIceTransport.
		// This is a transient state that may trigger intermittently (and resolve itself without action)
		// on a flaky network.
		// However:
		// The way this state is determined is implementation dependent.
		// Suggesting to handle reconnection only on Failed or Closed state instead.
		applog.FromContext(peer.ctx).Info("Peer disconnected, waiting to see if it recovers")
		break

	case webrtc.PeerConnectionStateConnected:
		peer.reconnectionScheduled = false

		// Theoretically there could be a situation when `webrtc.PeerConnection` does not gather
		// statistics yet when we entered `Connected` state as a webrtc.ICECandidatePairStats might be in a
		// webrtc.StatsICECandidatePairStateInProgress state here.

		var pair webrtc.ICECandidatePairStats
		candidates := make(map[string]webrtc.ICECandidateStats)

		for _, s := range peer.connection.GetStats() {
			switch stat := s.(type) {
			case webrtc.ICECandidateStats:
				candidates[stat.ID] = stat
			case webrtc.ICECandidatePairStats:
				if stat.State == webrtc.StatsICECandidatePairStateSucceeded {
					pair = stat
				}
			default:
			}
		}

		applog.Debug("Candidate pairs received, updating map")

		localCandidate, okLocal := candidates[pair.LocalCandidateID]
		remoteCandidate, okRemote := candidates[pair.RemoteCandidateID]
		if !okLocal || !okRemote {
			applog.FromContext(peer.ctx).Error("Could not find candidate pair in peer stats")
			return
		}

		// Ignoring resolve error as it shouldn't really happen as WebRTC will be putting
		// a valid IP address here.
		localAddress, _ := net.ResolveIPAddr("ip", localCandidate.IP)
		remoteAddress, _ := net.ResolveIPAddr("ip", remoteCandidate.IP)

		peer.localAddress = localAddress
		peer.remoteAddress = remoteAddress
		peer.localAddrReadyOnce.Do(func() {
			close(peer.localAddrReady)
		})

		applog.FromContext(peer.ctx).Info(
			"Local candidate",
			zap.Any("candidate", localCandidate),
		)
		applog.FromContext(peer.ctx).Info(
			"Remote candidate",
			zap.Any("candidate", remoteCandidate),
		)
		break
	default:
		break
	}
}

var manualReconnectionMessage icebreaker.CandidatesMessage

func (p *PeerManager) onPeerCandidatesGathered(remotePeer uint) onPeerCandidatesGatheredCallback {
	return func(description *webrtc.SessionDescription, candidates []webrtc.ICECandidate) {
		err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   icebreaker.EventKindCandidates,
					GameID:      p.gameId,
					SenderID:    p.localUserId,
					RecipientID: &remotePeer,
				},
				Session:    description,
				Candidates: candidates,
			})

			// A fake candidate message that is used in manual reconnection process
			// Will be fixed in future
			if manualReconnectionMessage.GameID == 0 {
				manualReconnectionMessage = icebreaker.CandidatesMessage{
					BaseEvent: icebreaker.BaseEvent{
						EventType:   icebreaker.EventKindCandidates,
						GameID:      p.gameId,
						SenderID:    p.localUserId,
						RecipientID: &remotePeer,
					},
					Session:    description,
					Candidates: candidates,
				}
				manualReconnectionMessage.Candidates = make([]webrtc.ICECandidate, 0)
			}

		if err != nil {
			applog.Error("Failed to send candidates",
				zap.Uint("playerId", p.localUserId),
				zap.Error(err),
			)
			go p.retrySendCandidates(remotePeer, description, candidates, 5)
		}
	}
}

func (p *PeerManager) retrySendCandidates(
	remote uint,
	desc *webrtc.SessionDescription,
	candidates []webrtc.ICECandidate,
	attempts int,
) {
	for i := 0; i < attempts && p.ctx.Err() == nil; i++ {
		d := time.Second * time.Duration(1<<i) // 1s, 2s, 4s, 8s, 16s
		select {
		case <-time.After(d):
		case <-p.ctx.Done():
			return
		}
		if err := p.icebreakerClient.SendEvent(
			icebreaker.CandidatesMessage{
				BaseEvent: icebreaker.BaseEvent{
					EventType:   icebreaker.EventKindCandidates,
					GameID:      p.gameId,
					SenderID:    p.localUserId,
					RecipientID: &remote,
				},
				Session:    desc,
				Candidates: candidates,
			}); err == nil {
			return
		}
	}
	p.scheduleReconnection(remote)
}

func (p *PeerManager) HandleGameDisconnected() {
	for _, peer := range p.peers {
		_ = peer.Close()
	}

	err := p.icebreakerClient.SendEvent(
		icebreaker.PeerClosingMessage{
			BaseEvent: icebreaker.BaseEvent{
				EventType: icebreaker.EventKindPeerClosing,
				GameID:    p.gameId,
				SenderID:  p.localUserId,
			},
		},
	)
	if err != nil {
		applog.Error("Failed to send closing event to icebreaker", zap.Error(err))
	}
}

func (p *PeerManager) HandleGameEnded() {
	for _, peer := range p.peers {
		peer.Disable()
	}
}

var watchers 	= make(map[uint]bool)
var watcherMutex  sync.Mutex

// There is a bug when we can't connect to a peer
// and connection process is getting stuck at "Peer exists but is inactive, scheduling reconnection"
// only happens at start, so no need to keep this thread alive for more than 2 min or so
// Note that this is a workaround and will require a proper fix in the future
//
// *Add. The bug described above is kinda fixed with one more workaround
// See: "Peer exists but is inactive". But some watcher is still needed so we run this at start for 2 min
func (p *PeerManager) initialConnectionWatcher(playerId uint) {
	peerAsString := fmt.Sprintf("%d", playerId)
	watcherMutex.Lock()
	if watchers[playerId] {
		applog.Debug("Watcher already exists. Peer: " + peerAsString, zap.Uint("playerId", playerId))
		watcherMutex.Unlock()
		return
	} else {
		applog.Debug("Creating connection watcher for peer " + peerAsString, zap.Uint("playerId", playerId))
		watchers[playerId] = true
		watcherMutex.Unlock()
	}

	peerConnnectedSecondsTotal := 0
	keepAliveSec := 120
	reconnectDelaySec := 60  // Ru players need 30+ seconds to establish connection        
	reconnectAttempts := 0
	isLeader := false

	if p.localUserId < playerId {
		isLeader = true
	}

	for i := 0; i < keepAliveSec; i++ {
		time.Sleep(time.Second)
		
		p.peersMu.Lock()
		if peer, ok := p.peers[playerId]; ok {
			if peer.connection.ConnectionState().String() == "connected"{
				peerConnnectedSecondsTotal += 1
			}
		}
		p.peersMu.Unlock()

		reconnectDelaySec -= 1
		if reconnectDelaySec == 0 {
			reconnectDelaySec = 90
			reconnectAttempts += 1

			// Sometimes connection go to "connected" state for a second
			// and then fails and get stuck. So if total time in "connected" state
			// is less than 2 seconds then we try to reconnect
			if peerConnnectedSecondsTotal < 2 {
				// odd tries - leader trying to reconnect
				// even - secondary peer
				if reconnectAttempts%2 == 0 {
					if !isLeader {
						applog.Info("Connection watcher: Initiating reconnection to peer: " + peerAsString, zap.Uint("playerId", playerId))
						p.handleReconnection(playerId, true)
					} else {
						applog.Info("Connection watcher: Skipping reconnection to peer: " + peerAsString, zap.Uint("playerId", playerId))
					}
				} else {
					if isLeader{
						applog.Info("Connection watcher: Initiating reconnection to peer: " + peerAsString, zap.Uint("playerId", playerId))
						p.handleReconnection(playerId, true)
					} else {
						applog.Info("Connection watcher: Skipping reconnection to peer: " + peerAsString, zap.Uint("playerId", playerId))
					}
				}
				
			}
		}
	}
	applog.Debug("Stopping connection watcher for peer " + peerAsString, zap.Uint("playerId", playerId))
	watcherMutex.Lock()
	watchers[playerId] = false
	watcherMutex.Unlock()
}

// Forcing "no relay" conn from UI. 
// A fallback scenario if TURNs lags too much and players can't get good pair
// No need to send any message to peer and wait
// Add: Doesn't work.
func (p *PeerManager) HandleManualNoRelayReconnRequest(playerId uint) {
	peerAsString := fmt.Sprintf("%d", playerId)
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	if ok {
		if !peer.manualReconnIsActive && !peer.remoteManualRRequest {
			peer.forceTurnRelay = false
			p.peersMu.Unlock()

			p.handleReconnection(playerId, true)
		} else {
			p.peersMu.Unlock()
			applog.Debug("Manual reconnection is already in process. Skipping local request. Peer: " + peerAsString)
		}
	} else {
		applog.Error("Can't initiate manual reconnection: No such peer " + peerAsString)
		p.peersMu.Unlock()
	}
	
}

func (p *PeerManager) HandleManualReconnectRequest(playerId uint) {
	peerAsString := fmt.Sprintf("%d", playerId)
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	if ok {
		if !peer.manualReconnIsActive {
			peer.manualReconnIsActive = true
			p.disableRecBtnPeers = append(p.disableRecBtnPeers, peerAsString)
			
			manualReconnectionMessage.BaseEvent.RecipientID = &playerId
			err := p.icebreakerClient.SendEvent(manualReconnectionMessage)
			if err != nil {
				applog.Error("Failed to send manualReconnectionMessage to peer " + peerAsString, zap.Error(err))
			}
			p.peersMu.Unlock()

			p.preparePeerForManualReconn(playerId)
			go p.scheduleManualReconnection(playerId)
		} else {
			p.peersMu.Unlock()
			applog.Debug("Manual reconnection is already in process. Skipping local request. Peer: " + peerAsString)
		}
	} else {
		applog.Error("Can't initiate manual reconnection: No such peer " + peerAsString)
		p.peersMu.Unlock()
	}
	
}

func (p *PeerManager) scheduleManualReconnection(playerId uint) {
	// Wait a few seconds before starting reconnection process
	// As we send a message to another peer but don't recieve an answer
	// we just assume that second peer recieved it and even if not, it doesn't really matter
	// since it will be using default turns instead of specific one and this is kinda ok too.
	time.Sleep(2*time.Second)

	p.peersMu.Lock()
	_, ok := p.peers[playerId]
	p.peersMu.Unlock()

	if ok {
		p.handleReconnection(playerId, false)
	}
}

func (p *PeerManager) handleRemoteManualReconnRequest(playerId uint) {
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]
	if ok {
		if !peer.manualReconnIsActive {
			peer.remoteManualRRequest = true
			p.disableRecBtnPeers = append(p.disableRecBtnPeers, fmt.Sprintf("%d", playerId))
			p.peersMu.Unlock()

			// No need to initiate reconn process when remote request is recieved
			// We prepare specific turn and then wait for reconn from other side
			p.preparePeerForManualReconn(playerId)
		} else {
			p.peersMu.Unlock()
		}
	} else {
		p.peersMu.Unlock()
	}
}

func (p *PeerManager) preparePeerForManualReconn(playerId uint) {
	peerAsString := fmt.Sprintf("%d", playerId)
	p.peersMu.Lock()
	peer, ok := p.peers[playerId]

	if ok {
		peer.numOfManualReconns += 1
		peer.peerSpecificTurn = nil

		numOfTurns := len(p.turnServer)
		totalCombinations := numOfTurns * numOfTurns
		isLeader := p.localUserId < playerId
		numOfReconns := peer.numOfManualReconns

		if numOfReconns > totalCombinations {
			numOfReconns = ((numOfReconns - 1) % totalCombinations) + 1
		}

		// Every manual reconnection attempt should give a different pair
		// So players can find a better connection if some TURNs are lagging
		// It tries pairs with same TURNs first, then mix them. 2 servers example:
		// 1. Cloud<->Cloud 2. Xirsys<->Xirsys 3. Cloud<->Xirsys 4. Xirsys<->Cloud
		if numOfReconns <= numOfTurns {
			peer.peerSpecificTurn = append(peer.peerSpecificTurn, p.turnServer[numOfReconns - 1])
			applog.Info("Specific TURN prepared. Leader: " + strconv.Itoa(numOfReconns) +
						" secondary: " + strconv.Itoa(numOfReconns) + " Peer: " + peerAsString + 
						" Number of manual reconnects: "+ strconv.Itoa(peer.numOfManualReconns))
			peer.specTurnIdLocal = numOfReconns
			peer.specTurnIdRemote = numOfReconns
		} else {
			leaderServerNum := 
				int(math.Ceil(float64((numOfReconns - numOfTurns)) / float64((totalCombinations - numOfTurns)/numOfTurns)))
			secondaryServerNum := (numOfReconns - numOfTurns) / leaderServerNum

			for i := 1; i <= numOfTurns; i++ {
				if i == leaderServerNum {
					secondaryServerNum += 1
					break
				} else if i == secondaryServerNum {
					break
				}
			}

			if isLeader {
				peer.peerSpecificTurn = append(peer.peerSpecificTurn, p.turnServer[leaderServerNum - 1])
				peer.specTurnIdLocal = leaderServerNum
				peer.specTurnIdRemote = secondaryServerNum
			} else {
				peer.peerSpecificTurn = append(peer.peerSpecificTurn, p.turnServer[secondaryServerNum - 1])
				peer.specTurnIdLocal = secondaryServerNum
				peer.specTurnIdRemote = leaderServerNum
			}

			applog.Info("Specific TURN prepared. Leader:" + strconv.Itoa(leaderServerNum) +
							" secondary: " + strconv.Itoa(secondaryServerNum) + " Peer: " + peerAsString)
		}
	}
	p.peersMu.Unlock()
}