package adapter

import (
	"context"
	"faf-pioneer/faf"
	"faf-pioneer/icebreaker"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"faf-pioneer/webrtc"
	"fmt"
	pionwebrtc "github.com/pion/webrtc/v4"
	"github.com/sirupsen/logrus"
	"strings"
)

type Adapter struct {
	gpgNetFromGame      chan *faf.GpgMessage
	gpgNetToGame        chan *faf.GpgMessage
	gpgNetToFafClient   chan *faf.GpgMessage
	gpgNetFromFafClient chan *faf.GpgMessage
	gameDataToGame      chan *[]byte
	icebreakerClient    *icebreaker.Client
	ctx                 context.Context
	launcherInfo        *launcher.Info
}

func New(ctx context.Context, info *launcher.Info) *Adapter {
	instance := &Adapter{
		ctx:                 ctx,
		launcherInfo:        info,
		gpgNetFromGame:      make(chan *faf.GpgMessage),
		gpgNetToGame:        make(chan *faf.GpgMessage),
		gpgNetToFafClient:   make(chan *faf.GpgMessage),
		gpgNetFromFafClient: make(chan *faf.GpgMessage),
		gameDataToGame:      make(chan *[]byte),
		icebreakerClient:    icebreaker.NewClient(info.ApiRoot, info.GameId, info.AccessToken),
	}

	return instance
}

func (a *Adapter) Start() error {
	// Wire GpgNetClient to GpgNetServer
	go func() {
		for msg := range a.gpgNetFromGame {
			a.gpgNetToFafClient <- msg
		}
	}()

	go func() {
		for msg := range a.gpgNetFromFafClient {
			a.gpgNetToGame <- msg
		}
	}()

	// Gather ICE servers and listen for WebRTC events
	sessionGameResponse, err := a.icebreakerClient.GetGameSession()

	if err != nil {
		return fmt.Errorf("could not query turn servers: %v", err)
	}

	channel := make(chan icebreaker.EventMessage)
	go a.icebreakerClient.Listen(channel)

	turnServer := make([]pionwebrtc.ICEServer, len(sessionGameResponse.Servers))
	for i, server := range sessionGameResponse.Servers {
		turnServer[i] = pionwebrtc.ICEServer{
			Username:       server.Username,
			Credential:     server.Credential,
			CredentialType: pionwebrtc.ICECredentialTypePassword,
			URLs:           make([]string, len(server.Urls)),
		}

		for j, url := range server.Urls {
			// for Java being Java reasons we unfortunately raped the URLs and need to convert it back
			turnServer[i].URLs[j] = strings.ReplaceAll(url, "://", ":")
		}
	}

	peerUdpPort, err := util.GetFreeUdpPort()
	if err != nil {
		return fmt.Errorf("failed to find free udp peer port: %v", err)
	}

	peerManager := webrtc.NewPeerManager(
		a.icebreakerClient,
		a.launcherInfo.UserId,
		a.launcherInfo.GameId,
		a.launcherInfo.GameUdpPort,
		peerUdpPort,
		turnServer,
		channel,
	)

	peerManager.Start()

	// Start the GPG-Net Control Server
	gpgNetServer := faf.NewGpgNetServer(a.ctx, &peerManager, a.launcherInfo.GpgNetPort)
	go func() {
		if err := gpgNetServer.Listen(a.gpgNetFromGame, a.gpgNetToGame); err != nil {
			logrus.WithError(err).Error("Failed to start listening GPG-Net control server connections")
		}
	}()

	return nil
}
