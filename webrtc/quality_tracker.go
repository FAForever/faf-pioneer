package webrtc

import (
	"sync"
	"time"
)

type ConnectionQuality struct {
	mu               sync.RWMutex
	lastConnectTime  time.Time
	connectionCount  int
	failureCount     int
	totalConnectTime time.Duration
	averageRTT       time.Duration
	packetLoss       float64
}

type QualityTracker struct {
	mu    sync.RWMutex
	peers map[uint]*ConnectionQuality
}

func NewQualityTracker() *QualityTracker {
	return &QualityTracker{
		peers: make(map[uint]*ConnectionQuality),
	}
}

func (qt *QualityTracker) GetPeerQuality(peerID uint) *ConnectionQuality {
	qt.mu.RLock()
	defer qt.mu.RUnlock()
	return qt.peers[peerID]
}

func (qt *QualityTracker) RecordConnectionStart(peerID uint) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	quality, exists := qt.peers[peerID]
	if !exists {
		quality = &ConnectionQuality{}
		qt.peers[peerID] = quality
	}

	quality.mu.Lock()
	quality.lastConnectTime = time.Now()
	quality.connectionCount++
	quality.mu.Unlock()
}

func (qt *QualityTracker) RecordConnectionSuccess(peerID uint) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	quality, exists := qt.peers[peerID]
	if !exists {
		return
	}

	quality.mu.Lock()
	connectDuration := time.Since(quality.lastConnectTime)
	quality.totalConnectTime += connectDuration
	quality.mu.Unlock()
}

func (qt *QualityTracker) RecordConnectionFailure(peerID uint) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	quality, exists := qt.peers[peerID]
	if !exists {
		return
	}

	quality.mu.Lock()
	quality.failureCount++
	quality.mu.Unlock()
}

func (qt *QualityTracker) UpdateRTT(peerID uint, rtt time.Duration) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	quality, exists := qt.peers[peerID]
	if !exists {
		quality = &ConnectionQuality{}
		qt.peers[peerID] = quality
	}

	quality.mu.Lock()
	// Simple moving average (could be improved with more sophisticated algorithms)
	if quality.averageRTT == 0 {
		quality.averageRTT = rtt
	} else {
		quality.averageRTT = (quality.averageRTT + rtt) / 2
	}
	quality.mu.Unlock()
}

func (qt *QualityTracker) UpdatePacketLoss(peerID uint, loss float64) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	quality, exists := qt.peers[peerID]
	if !exists {
		quality = &ConnectionQuality{}
		qt.peers[peerID] = quality
	}

	quality.mu.Lock()
	quality.packetLoss = loss
	quality.mu.Unlock()
}

// CalculateAdaptiveTimeout returns a timeout based on connection history
func (qt *QualityTracker) CalculateAdaptiveTimeout(peerID uint, baseTimeout time.Duration) time.Duration {
	quality := qt.GetPeerQuality(peerID)
	if quality == nil {
		return baseTimeout
	}

	quality.mu.RLock()
	defer quality.mu.RUnlock()

	// Base multiplier
	multiplier := 1.0

	// Increase timeout based on failure rate
	if quality.connectionCount > 0 {
		failureRate := float64(quality.failureCount) / float64(quality.connectionCount)
		if failureRate > 0.5 {
			multiplier += failureRate * 2.0 // Up to 3x for 100% failure rate
		}
	}

	// Increase timeout based on RTT
	if quality.averageRTT > 0 {
		if quality.averageRTT > 500*time.Millisecond {
			multiplier += 1.0 // Double for high RTT
		} else if quality.averageRTT > 200*time.Millisecond {
			multiplier += 0.5 // 1.5x for moderate RTT
		}
	}

	// Increase timeout based on packet loss
	if quality.packetLoss > 0.1 { // More than 10% loss
		multiplier += quality.packetLoss * 2.0
	}

	// Cap the maximum multiplier
	if multiplier > 5.0 {
		multiplier = 5.0
	}

	return time.Duration(float64(baseTimeout) * multiplier)
}

// GetFailureRate returns the failure rate for a peer
func (cq *ConnectionQuality) GetFailureRate() float64 {
	cq.mu.RLock()
	defer cq.mu.RUnlock()

	if cq.connectionCount == 0 {
		return 0.0
	}
	return float64(cq.failureCount) / float64(cq.connectionCount)
}

// GetAverageConnectTime returns the average time to establish a connection
func (cq *ConnectionQuality) GetAverageConnectTime() time.Duration {
	cq.mu.RLock()
	defer cq.mu.RUnlock()

	successfulConnections := cq.connectionCount - cq.failureCount
	if successfulConnections == 0 {
		return 0
	}
	return cq.totalConnectTime / time.Duration(successfulConnections)
}

// IsProblematic returns true if the peer has persistent connection issues
func (cq *ConnectionQuality) IsProblematic() bool {
	cq.mu.RLock()
	defer cq.mu.RUnlock()

	// Consider problematic if:
	// 1. High failure rate (>80%) with significant attempts (>3)
	// 2. Very high RTT (>2 seconds)
	// 3. High packet loss (>25%)
	return (cq.GetFailureRate() > 0.8 && cq.connectionCount > 3) ||
		cq.averageRTT > 2*time.Second ||
		cq.packetLoss > 0.25
}
