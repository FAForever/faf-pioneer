package dbgwnd

import (
	"faf-pioneer/applog"
	"faf-pioneer/adapter"
	"faf-pioneer/webrtc"
	"strings"
	pionwebrtc "github.com/pion/webrtc/v4"
	"strconv"
	"github.com/goforj/godump"
	"os"
	"time"
	"encoding/json"
	"slices"
	"fmt"
	. "modernc.org/tk9.0"
	_ "modernc.org/tk9.0/themes/azure"
)

const (
	disableRecButnOnNewConnect = 45 //seconds
	disableRecButnOnManualRec  = 20 //seconds
)

var pm                  *webrtc.PeerManager

var logStrings 			[]string
var formattedLogs		[]string
var rawLogs             []string
var sortedIds           []string
var allPeersStats       map[string]pionwebrtc.StatsReport
var connectionStates    map[string]string
var turnIds             map[string][]int
var turnsNameById       map[int]string
var idsToDisable        []string
var disabledBtnsTimers  = make(map[string]int)
var oldPeer             = make(map[string]bool)
var playerNames         = make(map[string]string)

var logFilePath = ""
var formattedLogsCount = 0
var rawLogCount = 0
var logViewLinesCount = 0
var numOfIdsInConnInfoTab = 0
var reconnectButtonsEnabled = 0
var treeViewNumOfLines = 0
var showRawLogs = false
var hideDebug = false
var clearLogWindow = false
var relaysUpdated = false
var region = ""

var myApp  = App
var forceRelayLabel     *LabelWidget
var regionLabel         *LabelWidget
var connTreeView        *TTreeviewWidget
var notebook            *TNotebookWidget
var logFrame            *TFrameWidget
var connInfoFrame       *TFrameWidget
var relayFrame          *TFrameWidget
var logView             *TextWidget
var logScroll           *TScrollbarWidget
var logXScroll          *TScrollbarWidget
var relayView           *TextWidget
var relayScroll         *TScrollbarWidget
var rawLogsChkButton 	*CheckbuttonWidget
var hideDebugChkButton  *CheckbuttonWidget
var connInfoView        *TextWidget
var connInfoScroll      *TScrollbarWidget
var connIdListBox       *ListboxWidget
var reconnectButtons    []*ButtonWidget
var reconnectNoRelayBtn []*ButtonWidget

var rawLogsChButtonVar  bool
var hideDebugButtonVar  bool


func CreateMainWindow(){
	myApp.WmTitle("FAF Pioneer")

	// Shows if "force relay" is ON or OFF
	forceRelayLabel = Label(Height(2), Anchor("w"), Txt("Force relay:"), Font("Helvetica", 12))

	// Region
	regionLabel = Label(Height(2), Anchor("w"), Txt("Version:"), Font("Helvetica", 12))

	// Connections treeview
	connTreeView = TTreeview(Selectmode("browse"), Columns("1 2 3 4 5 6 7"), Height(13))

	connTreeView.Column("#0", Anchor("center"), Width(120))
	connTreeView.Column(1, Anchor("center"), Width(120))
	connTreeView.Column(2, Anchor("center"), Width(100))
	connTreeView.Column(3, Anchor("center"), Width(40))
	connTreeView.Column(4, Anchor("center"), Width(120))
	connTreeView.Column(5, Anchor("center"), Width(120))
	connTreeView.Column(6, Anchor("center"), Width(180))
	connTreeView.Column(7, Anchor("center"), Width(180))

	connTreeView.Heading("#0", Txt("      ID"), Anchor("center"))
	connTreeView.Heading(1, Txt("Name"), Anchor("center"))
	connTreeView.Heading(2, Txt(" "), Anchor("center"))
	connTreeView.Heading(3, Txt(" "), Anchor("center"))
	connTreeView.Heading(4, Txt("Connection state"), Anchor("center"))
	connTreeView.Heading(5, Txt("RTT(ping)"), Anchor("center"))
	connTreeView.Heading(6, Txt("Local"), Anchor("center"))
	connTreeView.Heading(7, Txt("Remote"), Anchor("center"))

	// Reconnect buttons (15 buttons as max players count is 16)
	for i := 0; i < 15; i++ {
		but := Button(Txt("RECONNECT"), Font("Helvetica", 6, "bold"), Command(func() { reconnectBtnClicked(i) }))
    	reconnectButtons = append(reconnectButtons, but)

		padyFloat := 8 + float64(i)*4.5
		Grid(but, Row(1), Column(0), Sticky("NW"), Pady(fmt.Sprintf("%.1f", padyFloat)+"m 0m"),Padx("80m 0m"))
		but.Configure(State("disabled"))
	}

	// Reconnect NO RELAY buttons
	// for i := 0; i < 15; i++ {
	// 	but := Button(Txt(" ! "), Font("Helvetica", 6, "bold"), Command(func() { reconnNoRelaytBtnClicked(i) }))
    // 	reconnectNoRelayBtn = append(reconnectNoRelayBtn, but)

	// 	padyFloat := 8 + float64(i)*4.5
	// 	Grid(but, Row(1), Column(0), Sticky("NW"), Pady(fmt.Sprintf("%.1f", padyFloat)+"m 0m"),Padx("70m 0m"))
	// 	but.Configure(State("disabled"))
	// }

	
	// notebook contains different tabs (Logs, conn info etc.)
	notebook = TNotebook(Width(1100))
	logFrame = TFrame()
	connInfoFrame = TFrame()
	relayFrame = TFrame()

	notebook.Add(logFrame, Txt("Logs"))
	notebook.Add(connInfoFrame, Txt("Connection info"))
	notebook.Add(relayFrame, Txt("Relays"))
	GridRowConfigure(logFrame, 0, Weight(1))
	GridColumnConfigure(logFrame, 0, Weight(1))
	GridRowConfigure(connInfoFrame, 0, Weight(1))
	GridColumnConfigure(connInfoFrame, 0, Weight(1))
	GridRowConfigure(relayFrame, 0, Weight(1))
	GridColumnConfigure(relayFrame, 0, Weight(1))
	
	//Logs tab 
	logView = logFrame.Text(Wrap("none"), Setgrid(true), Yscrollcommand(
		func(e *Event) { e.ScrollSet(logScroll) }), Xscrollcommand(
		func(e *Event) { e.ScrollSet(logXScroll) }))
	logScroll = logFrame.TScrollbar(Command(func(e *Event) { e.Yview(logView) }))
	logXScroll = logFrame.TScrollbar(Command(func(e *Event) { e.Xview(logView) }), Orient("horizontal"))

	rawLogsChkButton = logFrame.Checkbutton(Txt("Show unformatted logs"), Variable(rawLogsChButtonVar), 
	Onvalue(true), Offvalue(false), Command(func() { rawLogsChkButtonPressed() }))

	hideDebugChkButton = logFrame.Checkbutton(Txt("Hide DEBUG"), Variable(hideDebugButtonVar), 
	Onvalue(true), Offvalue(false), Command(func() { hideDebugChkButtonPressed() }))

	Grid(logView, Row(0), Sticky("NSWE"))
	Grid(logScroll, Row(0), Column(1), Sticky("NSWE"), Pady("2m"))
	Grid(logXScroll, Row(1), Column(0), Sticky("NSWE"), Pady("2m"))
	Grid(rawLogsChkButton, Row(2), Column(0), Sticky("W"), Pady("1m"))
	Grid(hideDebugChkButton, Row(2), Column(0), Sticky("W"), Pady("1m"), Padx("50m"))

	//Connection info tab
	connInfoView = connInfoFrame.Text(Wrap("none"), Setgrid(true), Yscrollcommand(
		func(e *Event) { e.ScrollSet(connInfoScroll) }))
	connInfoScroll = connInfoFrame.TScrollbar(Command(func(e *Event) { e.Yview(connInfoView) }))
	connIdListBox = connInfoFrame.Listbox()
	Bind(connIdListBox, "<<ListboxSelect>>", Command(infoListIdSelected))

	Grid(connIdListBox, Row(0), Column(0), Sticky("NSW"))
	Grid(connInfoView, Row(0), Column(0), Columnspan(2), Sticky("NSWE"), Padx("40m 0m"))
	Grid(connInfoScroll, Row(0), Column(2), Sticky("NSWE"), Pady("2m"))

	// Relays tab
	relayView = relayFrame.Text(Wrap("none"), Setgrid(true), Yscrollcommand(
		func(e *Event) { e.ScrollSet(relayScroll) }))
	relayScroll = relayFrame.TScrollbar(Command(func(e *Event) { e.Yview(relayView) }))

	Grid(relayView, Row(0), Sticky("NSWE"))
	Grid(relayScroll, Row(0), Column(1), Sticky("NSWE"), Pady("2m"))

	// Main grid configuration
	GridRowConfigure(myApp, 0, Weight(0))
	GridRowConfigure(myApp, 1, Weight(0))
	GridRowConfigure(myApp, 2, Weight(10))

	Grid(forceRelayLabel, Row(0), Column(0), Sticky("NW"), Pady("0m 0m"), Padx("0m 0m"))
	Grid(regionLabel, Row(0), Column(0), Sticky("NW"), Pady("0m 0m"), Padx("50m 0m"))
	
	Grid(connTreeView, Row(1),  Column(0), Sticky("NW"), Pady("1m"), Ipadx("1m"), Ipady("1m"))
	Grid(notebook, Row(2), Column(0), Columnspan(2), Sticky("NSWE"))
	GridRowConfigure(myApp, 0, Weight(1))
	GridColumnConfigure(myApp, 0, Weight(1))

	ActivateTheme("azure light")

	refreshUI()

	Bind(myApp, "<Escape>", Command(func() { refreshConnStats() }))

	myApp.Wait()
}

func rawLogsChkButtonPressed(){
	clearLogWindow = true
	if rawLogsChkButton.Variable() == "true" {
		showRawLogs = true
	} else {
		showRawLogs = false
	}
}

func hideDebugChkButtonPressed(){
	clearLogWindow = true
	if hideDebugChkButton.Variable() == "true" {
		hideDebug = true
	} else {
		hideDebug = false
	}
}

func reconnNoRelaytBtnClicked(buttonID int){
	if len(sortedIds) > buttonID{
		if pm != nil {
			name := ""
			if n, ok := playerNames[sortedIds[buttonID]]; ok {
					name = n
			}
			answer := MessageBox(Detail("Are you sure you want to try to connect directly? This will expose your real IP address to peer: " +
				sortedIds[buttonID] + " " + name), Type("yesno"))
			connInfoView.Insert(END,  answer +" ", "", "\n")

			if answer == "yes"{
				playerID, _ := strconv.Atoi(sortedIds[buttonID])
				pm.HandleManualNoRelayReconnRequest(uint(playerID))
			}
		}
	}
}

func reconnectBtnClicked(buttonID int){
	applog.Info("UI: reconnect button " + fmt.Sprintf("%d", buttonID) + " is pressed. Sorted ids: " + godump.DumpJSONStr(sortedIds))
	if len(sortedIds) > buttonID{
		if pm != nil {
			playerID, _ := strconv.Atoi(sortedIds[buttonID])
			pm.HandleManualReconnectRequest(uint(playerID))
		}
	} else {
		applog.Error("UI: reconnect button id is out of range")
	}
}

func infoListIdSelected(){
	selected := connIdListBox.Curselection()

	if len(selected) > 0 {
		selectedId := connIdListBox.Get( strconv.Itoa(selected[0]))
		if len(selectedId) > 0 {
			stat, ok := allPeersStats[selectedId[0]]

			if ok {
				connInfoView.Clear()
				title := "Connection ID: " + selectedId[0] + " Time: " + time.Now().Format("15:04:05")

				connInfoView.Insert(END,  title, "", "\n")
				connInfoView.Insert(END,  godump.DumpJSONStr(stat) +" ", "", "\n")
			}
		}
	}
}

func refreshUI() {
	refreshLogs()
	refreshConnStats()
	TclAfter(time.Second * 1, refreshUI)
}

func refreshConnInfoListIds() {
	if numOfIdsInConnInfoTab != 0 {
		connIdListBox.Delete("0", strconv.Itoa(numOfIdsInConnInfoTab - 1))
	}
	numOfIdsInConnInfoTab = len(sortedIds)

	for i := len(sortedIds) - 1; i >= 0; i-- {
		connIdListBox.Insert(0, sortedIds[i])
	}
}

func refreshLogs() {
	textYpos,_ := strconv.ParseFloat(strings.Split(logView.Yview(), " ")[1], 64)

	if logFilePath == "" {
		logFilePath = applog.GetLogFilePath()
	}
	contentBytes, err := os.ReadFile(logFilePath)

	if err == nil {
		logStrings = nil
		fileContent := string(contentBytes)
		logStrings = strings.Split(fileContent,"\n")
		formatLogLines()
	}
	
	if clearLogWindow {
		logView.Clear()
		logViewLinesCount = 0
		clearLogWindow = false
	}

	if showRawLogs {
		for i, st := range rawLogs {
			if i < logViewLinesCount {
				continue
			}

			logViewLinesCount += 1
			logView.Insert(END, st+" ", "", "\n")
		}
	} else {
		for i, st := range formattedLogs {
			if i < logViewLinesCount {
				continue
			}

			logViewLinesCount += 1

			if hideDebug {
				if !strings.Contains(st, "DEBUG"){
					logView.Insert(END, st+" ", "", "\n")
				}
			} else {
				logView.Insert(END, st+" ", "", "\n")
			}
		}
	}
	

	//autoscroll when close to bottom and also on app start
	if textYpos > 0.99 || logViewLinesCount < 30{
		logView.Yviewmoveto(1)
	}
}

func formatLogLines() {
	if showRawLogs {
		for i, st := range logStrings {
			if i < rawLogCount {
				continue
			}

			// there should be no empty lines and if there is one
			// then something went wrong and we skip this try
			if len(st) < 5 {
				break
			}
			rawLogCount += 1
			rawLogs = append(rawLogs, st)
		}

	} else {
		for i, st := range logStrings {
			if i < formattedLogsCount {
				continue
			}

			if len(st) < 5 {
				break
			}

			formattedLogsCount += 1
			formattedLogs = append(formattedLogs, st)

			//replace loglevel with capital string
			formattedLogs[i] = strings.ReplaceAll(st,`"level":"info"`, "INFO")
			formattedLogs[i] = strings.ReplaceAll(formattedLogs[i],`"level":"debug"`, "DEBUG")
			formattedLogs[i] = strings.ReplaceAll(formattedLogs[i],`"level":"error"`, "ERROR")
			formattedLogs[i] = strings.ReplaceAll(formattedLogs[i],`"level":"warn"`, "WARN")

			//Replace date+time with local hh-mm-ss
			index := strings.Index(formattedLogs[i], `Z"`)
			if index != -1 {
				localTime := time.Now().Format("15:04:05")

				st1 := formattedLogs[i][:index-27] + localTime + formattedLogs[i][index+1:]
				formattedLogs[i] = st1
			}

			//Remove caller
			index = strings.Index(formattedLogs[i], `"caller"`)
			if index != -1 {
				ind2 := strings.Index(formattedLogs[i], `"msg"`)
				if ind2 != -1 {
					st1 := formattedLogs[i][:index] + formattedLogs[i][ind2+5:]
					formattedLogs[i] = st1
				}
			}

			//remove userID & localGameId
			if !strings.Contains(formattedLogs[i], `"Application started"`) {
				index = strings.Index(formattedLogs[i], `"localUserId"`)
				if index != -1{
					ind2 := strings.Index(formattedLogs[i], `"localGameId"`)
					if ind2 != -1 {
						indEnd1 := strings.Index(formattedLogs[i][ind2:], `,`)
						indEnd2 := strings.Index(formattedLogs[i][ind2:], `}`)
						indEndFinal := 0
						if indEnd1 == -1{
							indEndFinal = indEnd2
						} else if indEnd2 == -1 {
							indEndFinal = indEnd1
						} else {
							indEndFinal = min(indEnd1, indEnd2)
						}
					
						st1 := formattedLogs[i][:index] + formattedLogs[i][indEndFinal+ind2:]
						formattedLogs[i] = st1
					}
				}
			}
		}
	}	
}

func setRegion() {

}

func refreshConnStats(){
	if pm == nil {
		pm = adapter.GetPeerManager()
		regionLabel.Configure(Txt("Version:  " + adapter.GetRegion()))
	}
	
	if pm != nil {
		allPeersStats, connectionStates, turnIds, idsToDisable = pm.GetAllPeersStats()

		//sort all ids so the they always displayed in the right order
		sortedIds = nil
		sortedIdsInt := make([]int, 1)
		for id, _ := range(allPeersStats) {
			i,err := strconv.Atoi(id)
			if err == nil {
				sortedIdsInt = append(sortedIdsInt, i)
			}
		}
		slices.Sort(sortedIdsInt)
		for _,id := range(sortedIdsInt) {
			if id != 0{
				sortedIds = append(sortedIds, strconv.Itoa(id))
			}
		}

		if len(allPeersStats) != numOfIdsInConnInfoTab {
			refreshConnInfoListIds()
		}

		refreshButtons()

		if !relaysUpdated {
			relaysUpdated = true
			urls, turnNameById, forceRelay := pm.GetTurnURLs()
			turnsNameById = turnNameById

			if forceRelay {
				forceRelayLabel.Configure(Txt("Force relay: ON"))
			} else {
				forceRelayLabel.Configure(Txt("Force relay: OFF"))
			}

			for _, st := range urls {
				relayView.Insert(END, st+" ", "", "\n")
			}
		}
	}

	if len(allPeersStats) > 0 {
		// delete all lines in treeView. Idk if they are updatable or not
		// but for now we just delete all and add them again with new data
		for treeViewNumOfLines > 0 {
			connTreeView.Delete(strconv.Itoa(treeViewNumOfLines))
			treeViewNumOfLines -= 1
		}
		
		
		for _, id := range(sortedIds) {
			if _, ok := oldPeer[id]; !ok {
				oldPeer[id] = true
			}
			if stats, ok := allPeersStats[id]; ok {
				localCandidates := make(map[string]string)
				remoteCandidates := make(map[string]string)
				connectionState := "-"
				activeLocalCandidateId := ""
				activeRemoteCandidateId := ""
				activeLocalCandiType := "-"
				activeRemoteCandiType := "-"
				ping := "-"

				if cstate, ok := connectionStates[id]; ok {
					connectionState = cstate
				}
				
				for k,_ := range(stats) {
					//I'll take this data no matter what!
					//ARE YOU HEAR ME, GO?
					dataAsJSON := godump.DumpJSONStr(stats[k])
					var result map[string]interface{}
					err := json.Unmarshal([]byte(dataAsJSON), &result)

					if err == nil {
						if typ, ok := result["type"].(string); ok {
							if idVal,ok := result["id"].(string); ok {
								switch typ {
									case "local-candidate":
										if candidateType,ok := result["candidateType"].(string); ok {
											localCandidates[idVal] = candidateType
											if candidateIp,ok := result["ip"].(string); ok {
												localCandidates[idVal] += " "+ candidateIp
											}
										}
									case "remote-candidate":
										if candidateType,ok := result["candidateType"].(string); ok {
											remoteCandidates[idVal] = candidateType
											if candidateIp,ok := result["ip"].(string); ok {
												remoteCandidates[idVal] += " "+ candidateIp
											}
										}
									case "candidate-pair":
										if nominated,ok := result["nominated"].(bool); ok {
											if nominated {
												if localCandi,ok := result["localCandidateId"].(string); ok {
													activeLocalCandidateId = localCandi
												}
												if remoteCandi,ok := result["remoteCandidateId"].(string); ok {
													activeRemoteCandidateId = remoteCandi
												}
											
												if rtt, ok := result["currentRoundTripTime"].(float64); ok {
													ping = strconv.Itoa(int(rtt * 1000))
												}
											}
										}	
								}
							}
						}
					}
					
				}

				localTurnName := ""
				remoteTurnName := ""

				if tIds, ok := turnIds[id]; ok {
					if len(tIds) > 1 {
						if tIds[0] != 0 {
							localTurnName = turnsNameById[tIds[0]]
						}
						if tIds[1] != 0 {
							remoteTurnName = turnsNameById[tIds[1]]
						}
					}
				}
				
				if localCandiType, ok := localCandidates[activeLocalCandidateId]; ok {
					activeLocalCandiType = localCandiType + " " + localTurnName
				}
				if remoteCandiType, ok := remoteCandidates[activeRemoteCandidateId]; ok {
					activeRemoteCandiType = remoteCandiType + " " + remoteTurnName
				}
				
				timer := ""
				if _, ok := disabledBtnsTimers[id]; ok {
					timer = strconv.Itoa(disabledBtnsTimers[id])+"s"
				}

				playerName := "-"
				if name, ok := playerNames[id]; ok {
					playerName = name
				} else {
					playerNames = adapter.GetNicknames()
				}

				treeViewNumOfLines += 1

				//https://gitlab.com/cznic/tk9.0/-/blob/master/themes/azure/_examples/example.go#L205
				tvData := []any{"", treeViewNumOfLines, id,
					"{"+playerName+"}"+" "+"{}"+" "+"{"+timer+"}"+" "+"{"+connectionState+"}"+" "+"{"+ping+"}"+" "+"{"+activeLocalCandiType+"}"+" "+"{"+activeRemoteCandiType+"}"}

				connTreeView.Insert(tvData[0], "end", Id(tvData[1]), Txt(tvData[2]), Value(tvData[3]))
			}
		}
	}
}

func refreshButtons() {
	numOfIds := len(sortedIds)

	if reconnectButtonsEnabled != numOfIds {
		// normal buttons
		for i,b := range(reconnectButtons){
			reconnectButtonsEnabled += 1
			if i < numOfIds {
				b.Configure((State("normal")))
			} else {
				b.Configure((State("disabled")))
			}
		}

		// no relay "!" buttons
		// for i,b := range(reconnectNoRelayBtn){
		// 	if i < numOfIds {
		// 		b.Configure((State("normal")))
		// 	} else {
		// 		b.Configure((State("disabled")))
		// 	}
		// }
	}

	if len(oldPeer) < len(sortedIds) {
		for i,_ := range(sortedIds) {
			if _, ok := oldPeer[sortedIds[i]]; !ok {
				disabledBtnsTimers[sortedIds[i]] = disableRecButnOnNewConnect
			}
		}
	}

	if len(idsToDisable) > 0 || len(disabledBtnsTimers) > 0 {
		for i,_ := range(disabledBtnsTimers) {
			if disabledBtnsTimers[i] > 0 {
				disabledBtnsTimers[i] -= 1
			} else {
				delete(disabledBtnsTimers, i)
			}
		}

		for i,_ := range(sortedIds) {
			if slices.Contains(idsToDisable, sortedIds[i]){
				disabledBtnsTimers[sortedIds[i]] = disableRecButnOnManualRec
			}

			// normal buttons
			if _, ok := disabledBtnsTimers[sortedIds[i]]; ok {
				reconnectButtons[i].Configure((State("disabled")))
			} else {
				reconnectButtons[i].Configure((State("normal")))
			}

			// no relay "!" buttons
			// if _, ok := disabledBtnsTimers[sortedIds[i]]; ok {
			// 	reconnectNoRelayBtn[i].Configure((State("disabled")))
			// } else {
			// 	reconnectNoRelayBtn[i].Configure((State("normal")))
			// }
		}
		
		idsToDisable = nil
	}
}