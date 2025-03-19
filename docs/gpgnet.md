# GPGnet Protocol

## Overview
The GPGnet protocol is used to instruct the `Forged Alliance.exe` binary with information about what lobby to open, which players to connect, game options, game results and potentially much more.

GPGnet is used across the apps from game to lobby server:
```
[FA.exe] <--> [faf-pioneer] <--> [faf-client] <--> [faf-lobby-server]
```

The correct order of sending is crucial in order to get players into a lobby and connected to each other.

But the GPGnet protocol can also be extended by SIM mods in the game since the message are sent in Lua code. However: A running game can never receive or process GPGnet messages, once start, it is sending only.

## Binary representation

The game sends and receives GPGnet messages in a custom representation. The encoding is always Little Endian (but affects only uint32 fragments).

Here is the specification in Backus-Naur-Form:
```
<message>              ::= <command> <size> <chunks>
<command>              ::= <string>
<size>                 ::= uint32
<string>               ::= <size> []byte
<chunks>               ::= <chunk> | <chunk> <chunks>
<chunk>                ::= <type_int> uint32 | <type_string> <string> | <type_followup_string> <string>
<type_int>             ::= byte = 0x00
<type_string>          ::= byte = 0x01
<type_followup_string> ::= byte = 0x02
```

## Known messages types

| **command**        | **sent by** | **args** | **index** | **type** | **content**                                  |
|--------------------|-------------|----------|-----------|----------|----------------------------------------------|
| GameState          | game        | 1        | 0         | str      | state: [Idle,Lobby, ???]                     |
| CreateLobby        | client      | 5        | 0         | int      | lobby init mode 0 = Normal / Custom 1 = Auto |
|                    |             |          | 1         | int      | lobby port                                   |
|                    |             |          | 2         | str      | local player name                            |
|                    |             |          | 3         | int      | local player id                              |
|                    |             |          | 4         | int      | unknown                                      |
| HostGame           | client      | 1        | 0         | str      | map name                                     |
| JoinGame           | client      | 3        | 0         | str      | net address                                  |
|                    |             |          | 1         | str      | remote player name                           |
|                    |             |          | 2         | int      | remote player id                             |
| ConnectToPeer      | client      | 3        | 0         | str      | net address                                  |
|                    |             |          | 1         | str      | remote player name                           |
|                    |             |          | 2         | int      | remote player id                             |
| DisconnectFromPeer | client      | 1        | 0         | int      | remote player id                             |
| GameOptions        | both?       | ?        | ?         | ?        | ?                                            |
| GameEndedMessage   | game        | 0        |           |          |                                              |


## Message flow for opening a lobby

To create a lobby we need `[FA.exe]`, `[faf-pioneer]` and a `[faf-client]`. We will assume that `[faf-lobby-server]`
are integrated into `[faf-client]`.

For example, we have two users **in LAN** connecting to each user forming a lobby. 
Connection process will be the following:

<table>
<tr>
    <td><b>Stage</b></td>
    <td><b>source</b></td>
    <td><b>destination</b></td>
    <td><b>gpgnet.Message</b></td>
    <td><b>args / comments</b></td>
</tr>
<tr>
    <td>1</td>
    <td><b>UserA</b></td>
    <td></td>
    <td></td>
    <td>

Starts `[faf-client]`  
    </td>
</tr>
<tr>
    <td>2</td>
    <td><b>UserA</b></td>
    <td></td>
    <td></td>
    <td>
    
`[faf-client]` spawns `[faf-pioneer]`
    </td>
</tr>
<tr>
    <td>3</td>
    <td><b>UserB</b></td>
    <td></td>
    <td></td>
    <td>

Starts `[faf-client]`  
    </td>
</tr>
<tr>
    <td>4</td>
    <td><b>UserB</b></td>
    <td></td>
    <td></td>
    <td>
    
`[faf-client]` spawns `[faf-pioneer]`
    </td>
</tr>
<tr>
    <td>5</td>
    <td><b>UserA</b></td>
    <td>

**UserA**'s `[FA.exe]`
</td>
    <td>

`CreateLobbyMessage`
</td>
    <td>

Sends a `CreateLobbyMessage` packet to `[FA.exe]`:
```go
return &CreateLobbyMessage{
    LobbyInitMode:    gpgnet.LobbyInitModeNormal,
    LobbyPort:        14080,
    LocalPlayerName:  "UserA",
    LocalPlayerId:    1,
    UnknownParameter: 1,
}
```

</td>
</tr>
<tr>
    <td>6</td>
    <td><b>UserA</b></td>
    <td>

**UserA**'s `[FA.exe]`
</td>
    <td>

`HostGameMessage`
</td>
    <td>

Sends a `HostGameMessage` packet to `[FA.exe]`:
```go
return &HostGameMessage{
    MapName: "",
}
```
</td>
</tr>
<tr>
    <td>7</td>
    <td><b>UserB</b></td>
    <td>

**UserB**'s `[FA.exe]`
</td>
    <td>

`CreateLobbyMessage`
</td>
    <td>

Sends a `CreateLobbyMessage` packet to `[FA.exe]`:
```go
return &CreateLobbyMessage{
    LobbyInitMode:    gpgnet.LobbyInitModeNormal,
    LobbyPort:        14081,
    LocalPlayerName:  "UserB",
    LocalPlayerId:    2,
    UnknownParameter: 1,
}
```
</td>
</tr>
<tr>
    <td>8</td>
    <td><b>UserB</b></td>
    <td>

**UserB**'s `[FA.exe]`
</td>
    <td>

`JoinGameMessage`
</td>
    <td>

Sends a `JoinGameMessage` packet to `[FA.exe]`:
```go
return &JoinGameMessage{
    RemotePlayerLogin: "UserA",
    RemotePlayerId:    1,
    Destination:       "127.0.0.1:14080",
}
```
</td>
</tr>
<tr>
    <td>9</td>
    <td><b>UserA</b></td>
    <td>

**UserA**'s `[FA.exe]`
</td>
    <td>

`ConnectToPeerMessage`
</td>
    <td>

Sends a `ConnectToPeerMessage` packet to `[FA.exe]`:
```go
return &ConnectToPeerMessage{
    RemotePlayerLogin: "UserB",
    RemotePlayerId:    2,
    Destination:       "127.0.0.1:14081",
}
```
</td>
</tr>
</table>

After receiving `ConnectToPeerMessage` command game initiates a UDP connection to port that we specify in `Destination`
field of that packet.  
In case of `[faf-pioneer]` this port should be UDP-Proxy port that will transfer all game data via WebRTC.  
We can set `127.0.0.1:0` as a destination as we are swapping ports by replacing `JoinGameMessage` and 
`ConnectToPeerMessage` commands that launcher forwards to our `[FA.exe]` (game). Since we will establish
Peer connection via WebRTC and GameUDPProxy will be started as well, it will be replaced to `localPort`.

### Full description

1. **[ UserA ]** starts a `[faf-client]` that begin listening GPG-Net client port (`gpgnet-client-port`) 
with following arguments:
   - `--user-id 1` - ID of a current user in a lobby, host will be first, so it's **1**.
   - `--user-name UserA` - Username, here only for example purposes, because in a normal launcher
   we will receive user information during the login.
   - `--game-id 100` - ID of a current game/lobby.
   - `--gpgnet-port 21000` - Local GPG-Net control server port.
   - `--gpgnet-client-port 21005` - Local GPG-Net client port that is used to communicate between `[faf-client]`
   and `[faf-pioneer]`.
   - `--game-udp-port 14080` - Game UDP port that will be used by `[FA.exe]`.
   - `--api-root "http://localhost:8080"` - API root for ICE-Breaker server (not set in real launcher).
2. **[ UserA ]** starts a `[faf-pioneer]` (in normal world, a `[faf-client]` should spawn a `[faf-pioneer]`
process in the operating system) with following arguments:  
   - `--user-id 1`
   - `--user-name UserA`
   - `--game-id 100`
   - `--gpgnet-port 21000`
   - `--gpgnet-client-port 21005`
   - `--game-udp-port 14080`
   - `--api-root "http://localhost:8080"`
3. **[ UserB ]** starts a `[faf-client]` the same way as _UserA_, except that:
   - `--user-id 2` - User ID will be **2** instead, for 3rd user it will be **3** and so on.
   - `--user-name UserB` - Username, again, only for example of `faf-launcher-emulator` command.
   - `--gpgnet-port 21001`
   - `--gpgnet-client-port 21006`
   - `--game-udp-port 14081`
4. **[ UserB ]** starts a `[faf-pioneer]` with same arguments as on a step 3.

Assuming that both users are started their `[faf-client]` and `[faf-pioneer]`, process continues.

5. **[ UserA ]** sends a GPG-Net message `CreateLobbyMessage[]` from `[faf-client]` to `[faf-pioneer]`, 
which writes it to `fafClientToAdapter` channel and then to forward it to `[FA.exe]`, in code it looks like:
    ```go
    s.sendMessage(gpgnet.NewCreateLobbyMessage(
        gpgnet.LobbyInitModeNormal,
        // LocalGameUdpPort, will be `14080`
        int32(s.server.info.GameUdpPort),
        // Username, will be `UserA`
        s.server.info.UserName,
        // Player/User ID, will be `1`
        int32(s.server.info.UserId),
    ))
    ```
6. **[ UserA ]** sends a GPG-Net message `HostGame[map=""]` from `[faf-client]` to `[faf-pioneer]`, which writes it to
`fafClientToAdapter` channel and then to forward it to `[FA.exe]`.
7. **[ UserB ]** sends a GPG-Net message `CreateLobbyMessage[]` from `[faf-client]` to `[faf-pioneer]`,
   which writes it to `fafClientToAdapter` channel and then to forward it to `[FA.exe]`, in code it looks like:
    ```go
    s.sendMessage(gpgnet.NewCreateLobbyMessage(
        gpgnet.LobbyInitModeNormal,
        // LocalGameUdpPort, will be `14081`
        int32(s.server.info.GameUdpPort),
        // Username, will be `UserB`
        s.server.info.UserName,
        // Player/User ID, will be `2`
        int32(s.server.info.UserId),
    ))
    ```
8. **[ UserB ]** sends a GPG-Net message `JoinGame[map=""]` from `[faf-client]` to `[faf-pioneer]`, which writes it to
   `fafClientToAdapter` channel and then to forward it to `[FA.exe]`.
    ```go
    server.SendMessagesToGame(
        gpgnet.NewJoinGameMessage(
            "UserA",
            1,
            fmt.Sprintf("127.0.0.1:%d", 14080),
        ),
    )
    ```
9. **[ UserA ]** sends a GPG-Net message `ConnectToPeer[]` from `[faf-client]` to `[faf-pioneer]`,
   which writes it to `fafClientToAdapter` channel and then to forward it to `[FA.exe]`, in code it looks like:
    ```go
    server.SendMessagesToGame(
        gpgnet.NewConnectToPeerMessage(
            "UserB",
            2,
            fmt.Sprintf("127.0.0.1:%d", 14081),
        ),
    )
    ```
