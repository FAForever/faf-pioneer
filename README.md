# faf-pioneer
FAF ICE Adapter based on [WebRTC](https://webrtc.org/) and the [Pion project](https://github.com/pion).
To understand the problem we are trying to solve, please check our [network architecture](docs/network_architecture.md).


<details>

<summary>FAF Pioneer application architecture</summary>

![application-architecture.svg](docs/diagrams/application-architecture.svg)

</details>

#### Based on modern standards

WebRTC is a specification to send and receive audio, video and any other data to be passed in a peer to peer setup with other users. It used by every modern audio/video conferencing software like Zoom, MS Teams, Google Meeet and many more.

The Pion library is a first class implementation of the WebRTC spec. The faf-pioneer tries to establish connections between players via ICE (a protocol use by WebRTC) and sends the game data over "data channels".


If you want to learn more about it, WebRTC has a is explained very thoroughly in [webrtcforthecurious.com](https://webrtcforthecurious.com/)

## How to run & test

**!!! This section is subject to change as we are still in a very early stage. !!!**

The docker-compose.yaml provides you with all services you need to run & test this application.

* The faf-icebreaker as our signalling server and turn server provider (along with MariaDB & RabbitMQ as dependencies)
* Eturnal as a STUN and TURN server

To set up everything just run

```shell script
docker compose up -d
```
This will start all services in the right order and with default configuration.

Now you need to run the faf-pioneer on 2 pcs / VMs. Both need to be able to contact the faf-icebreaker application. You can expose the app via tools like Ngrok.

So on local machine you run it via
```shell script
./run_test_as.sh 1
```

On the remote end you have to specify the icebreaker API. This is an example:
````shell script
API_ROOT=<ngrok-url>> ./run_test_as.sh 2
````

Now to receive data run netcat to listen on port 60000. To send data to the other side send it via netcat to localhost port 18ßßß.
Always use UDP here! Also, dependening on your version, you might need to specify IPv4.

### Testing with launcher

#### Using native FAF-Client
1. Download and checkout `pioneer` branch for [downlords-faf-client](https://github.com/FAForever/downlords-faf-client/tree/pioneer) repository.
2. Set the launcher environment variable `PIONEER_BIN_NAME` to output binary of `faf-ice`.

#### Using go-emulation of FAF-Client

1. Run the [faf-launcher-emulator](./cmd/faf-launcher-emulator/main.go) with the same arguments 
as [faf-ice](./cmd/faf-ice/main.go)
2. Run the [faf-ice](./cmd/faf-ice/main.go)
3. Start the game, an example:
```
cd /D C:\ProgramData\FAForever\bin
ForgedAlliance.exe /init init.lua /nobugreport /gpgnet 127.0.0.1:21000 /numgames 11 /numgames 12 /log "C:\ProgramData\FAForever\logs\test.log"
```

## Data flow

There is a continuous data flow between this application, the Forged Alliance game, the FAF client and (obviously) the whole stack of these 3 applications on other players computers.

More details can be found in [GPGnet protocol docs](docs/gpgnet.md).

### Local launch order
(Work in Progress)
1. The FAF client launches this ICE adapter and waits for it to signal readiness.
    * In prior iterations this was due to the ICE adapter running an RPC server and the FAF client successfully connecting to it. 
2. The FAF client launches the Forged Alliance game.
3. The Forged Alliance game connects to the TCP server for the GpgNet port, that the FAF client has passed to both the ICE adapter and the game.
4. The Forged Alliance game send a `GameState=Idle` message to the ICE adapter.
5. The FAF client tells the ICE adapter to host or join a game (using `CreateLobby` and right after it `HostGame` packets).
6. The ICE adapter tells the game to open a lobby with a predefined udp port.
7. The FAF client tells the game through the ICE adapter whom to connect to.


## Glossary

Due to the long arching history of connectivity among the history of the Forged Alliance Forever, a confusing terminology has established.

* ICE Adapter
  * An adapter application between the FAF client and the game that orchestrates the communication so that connectivity to other players is done externally (in this iteration via WebRTC, in prior iteration via ICE and local socket handling).
* Icebreaker
  * The new signalling server.
  * Grants access to the list of available STUN and TURN servers along with session based credentials.
* Lobby server
  * The service that the FAF client uses to manage game lobbies and initiate information on who plays with whom. In prior ICE adapter iterations it also had the role of the signalling server proxied via the FAF client.
* RPC connection
  * In prior iterations the FAF client communicated with the ICE adapter via JSON-RPC 2.0
  * The ICE adapter was the host.
* GpgNet
  * A binary TCP protocol for exchanging messages between the game and the lobby server.
  * The ICE adapter is acting as a server and the game connects to it. The port is specified as launch parameter of both applications.
  * Due to WebRTC/ICE changing ports and ip addresses, the ICE adapter has to read, intercept and modify some of the messages.
  * The protocol can be extended for more messages via SIM mods or game patches. Thus the adapter needs to handle and pass through all unknown messages to ensure compatibility.

## Contribute

This is the first Go application in FAForever project. We have absolutely no Go experience so far. If we do not follow best practices and you have improvements, all help is welcome.