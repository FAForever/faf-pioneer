package launcher

import (
	"flag"
	"fmt"
)

type Info struct {
	UserId           uint
	UserName         string
	GameId           uint64
	AccessToken      string
	ApiRoot          string
	GpgNetPort       uint
	GpgNetClientPort uint
	GameUdpPort      uint
	ForceTurnRelay   bool
}

func NewInfoFromFlags() *Info {
	userId := flag.Uint(
		"user-id", 0, "The ID of the user")
	userName := flag.String(
		"user-name", "", "The name of the user")
	gameId := flag.Uint64(
		"game-id", 0, "The ID of the game session")
	accessToken := flag.String(
		"access-token", "", "The access token for authentication")
	apiRoot := flag.String(
		"api-root", "https://api.faforever.com/ice", "The root uri of the icebreaker api")
	gpgNetPort := flag.Uint(
		"gpgnet-port", 0, "The port which the game will connect to for exchanging GgpNet messages")
	gpgNetClientPort := flag.Uint(
		"gpgnet-client-port", 0, "The port which on which the parent FAF client listens on")
	gameUdpPort := flag.Uint(
		"game-udp-port", 0, "The port which the game will send/receive game data")
	forceTurnRelay := flag.Bool(
		"force-turn-relay", false, "Force TURN relay using WebRTC")

	flag.Parse()

	return &Info{
		UserId:           *userId,
		UserName:         *userName,
		GameId:           *gameId,
		AccessToken:      *accessToken,
		ApiRoot:          *apiRoot,
		GpgNetPort:       *gpgNetPort,
		GpgNetClientPort: *gpgNetClientPort,
		GameUdpPort:      *gameUdpPort,
		ForceTurnRelay:   *forceTurnRelay,
	}
}

func (c *Info) Validate() error {
	if c.UserId == 0 {
		return fmt.Errorf("--user-id is required and must be a valid uint32")
	}

	if c.GameId == 0 {
		return fmt.Errorf("--game-id is required and must be a valid uint64")
	}

	if c.AccessToken == "" {
		return fmt.Errorf("--access-token is required and cannot be empty")
	}

	if c.GpgNetPort == 0 {
		return fmt.Errorf("--gpgnet-port is required and cannot be empty")
	}

	return nil
}
