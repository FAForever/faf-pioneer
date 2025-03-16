package icebreaker

type SessionTokenRequest struct {
	GameId uint64 `json:"gameId"`
}

type SessionTokenResponse struct {
	Jwt string `json:"jwt"`
}

type SessionGameResponse struct {
	Id      string                      `json:"id"`
	Servers []SessionGameResponseServer `json:"servers"`
}

type SessionGameResponseServer struct {
	Id         string   `json:"id"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
	Urls       []string `json:"urls"`
}
