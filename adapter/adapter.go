package adapter

import (
	"context"
	"faf-pioneer/applog"
	"faf-pioneer/faf"
	"faf-pioneer/gpgnet"
	"faf-pioneer/icebreaker"
	"faf-pioneer/launcher"
	"faf-pioneer/util"
	"faf-pioneer/webrtc"
	"fmt"
	pionwebrtc "github.com/pion/webrtc/v4"
	"go.uber.org/zap"
	"strings"
)

type Adapter struct {
	gpgNetFromGame      chan gpgnet.Message
	gpgNetToGame        chan gpgnet.Message
	gpgNetToFafClient   chan gpgnet.Message
	gpgNetFromFafClient chan gpgnet.Message
	gameDataToGame      chan *[]byte
	icebreakerClient    *icebreaker.Client
	ctx                 context.Context
	launcherInfo        *launcher.Info
}

func New(ctx context.Context, info *launcher.Info) *Adapter {
	instance := &Adapter{
		ctx:                 ctx,
		launcherInfo:        info,
		gpgNetFromGame:      make(chan gpgnet.Message),
		gpgNetToGame:        make(chan gpgnet.Message),
		gpgNetToFafClient:   make(chan gpgnet.Message),
		gpgNetFromFafClient: make(chan gpgnet.Message),
		gameDataToGame:      make(chan *[]byte),
		icebreakerClient:    icebreaker.NewClient(ctx, info.ApiRoot, info.GameId, info.AccessToken),
	}

	return instance
}

func (a *Adapter) Start() error {
	// Gather ICE servers and listen for WebRTC events
	sessionGameResponse, err := a.icebreakerClient.GetGameSession()
	if err != nil {
		return fmt.Errorf("could not query turn servers: %v", err)
	}

	iceBreakerEventChannel := make(chan icebreaker.EventMessage)
	go func() {
		if err = a.icebreakerClient.Listen(iceBreakerEventChannel); err != nil {
			applog.Error("could not start listening ICE-Breaker API (server-side) events", zap.Error(err))
		}
	}()

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

	for _, server := range sessionGameResponse.Servers {
		applog.Debug("Turn server", zap.Strings("urls", server.Urls))
	}

	peerUdpPort, err := util.GetFreeUdpPort()
	if err != nil {
		return fmt.Errorf("failed to find free udp peer port: %v", err)
	}

	if a.launcherInfo.ForceTurnRelay {
		applog.Debug("Forcing TURN relay on")
	}

	applog.Debug("Selected UDP game port", zap.Uint("gamePort", peerUdpPort))

	peerManager := webrtc.NewPeerManager(
		a.ctx,
		a.icebreakerClient,
		a.launcherInfo,
		peerUdpPort,
		turnServer,
		iceBreakerEventChannel,
	)

	gpgNetServer := faf.NewGpgNetServer(a.ctx, peerManager, a.launcherInfo.GpgNetPort)
	gpgNetClient := faf.NewGpgNetClient(a.ctx, a.launcherInfo.GpgNetClientPort)

	// Redirect messages from FAF.exe to FAF-Client
	go util.RedirectChannelWithContext(a.ctx, a.gpgNetFromGame, a.gpgNetToFafClient)
	// Redirect messages from FAF-Client to FAF.exe
	go func() {
		for {
			select {
			case msg, ok := <-a.gpgNetFromFafClient:
				if !ok {
					return
				}

				if baseMsg, isBase := msg.(*gpgnet.BaseMessage); isBase {
					parsedMsg, parseErr := baseMsg.TryParse()
					if parseErr == nil {
						processed := gpgNetServer.ProcessMessage(parsedMsg)
						a.gpgNetToGame <- processed
						continue
					}
				}

				a.gpgNetToGame <- msg
			case <-a.ctx.Done():
				return
			}
		}
	}()

	// Start the GPG-Net control server that acts like a primary bridge between game and this network adapter.
	go func() {
		if err := gpgNetServer.Listen(a.gpgNetFromGame, a.gpgNetToGame, peerUdpPort); err != nil {
			applog.Error("Failed to start listening GPG-Net control server connections", zap.Error(err))
		}
	}()

	// Start the GPG-Net client that will proxy data from game to FAF-Client.
	go func() {
		if err := gpgNetClient.Connect(a.gpgNetToFafClient, a.gpgNetFromFafClient); err != nil {
			applog.Error("Failed to start listening GPG-Net client proxy connections", zap.Error(err))
		}
	}()

	peerManager.Start()
	return nil
}
