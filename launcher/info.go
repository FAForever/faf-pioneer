package launcher

import (
	"flag"
	"fmt"
)

type Info struct {
	UserId            uint
	UserName          string
	GameId            uint64
	AccessToken       string `json:"-"`
	ApiRoot           string
	GpgNetPort        uint
	GpgNetClientPort  uint
	ForceTurnRelay    bool
	ConsentLogSharing bool
	LogLevel          int
	LogPath           string
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
	forceTurnRelay := flag.Bool(
		"force-turn-relay", false, "Force TURN relay using WebRTC")
	consentLogSharing := flag.Bool(
		"consent-log-sharing", false, "Consent log sharing")
	logLevel := flag.Int(
		"log-level", 0, "Log level: -1 - Trace, 0 - Info, 1 - Error, 2/3 - Panic, 4 - Fatal")
	logPath := flag.String(
		"log-path",
		"",
		"Directory to the logs, otherwise will use working directory and add 'logs' to that path")

	flag.Parse()

	return &Info{
		UserId:            *userId,
		UserName:          *userName,
		GameId:            *gameId,
		AccessToken:       *accessToken,
		ApiRoot:           *apiRoot,
		GpgNetPort:        *gpgNetPort,
		GpgNetClientPort:  *gpgNetClientPort,
		ForceTurnRelay:    *forceTurnRelay,
		ConsentLogSharing: *consentLogSharing,
		LogLevel:          *logLevel,
		LogPath:           *logPath,
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
