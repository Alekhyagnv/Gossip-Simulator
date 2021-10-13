#time "on"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.TestKit"

open System
open System.Collections.Generic
open Akka.Actor
open Akka.FSharp
open System.Diagnostics
open Akka.Configuration

let args: string array = fsi.CommandLineArgs |> Array.tail
let mutable noOfNodes = args.[0] |> int
let topologyT = args.[1]
let algo = args.[2]
let system = ActorSystem.Create("TopologySystem")
let tmr = Stopwatch()
let randomR = System.Random()
let mutable gSize = 0
let mutable faultNPercentage = args.[3] |> int
let failureNodes = int (faultNPercentage * noOfNodes / 100)
let mutable actorsFinished = new List<int>()

type Simulator =
    | PushSumFinished
    | GossipProtocolFinished  of (int)
    | PopNeighbors of (int * IActorRef * IActorRef [])
    | InitializeGossipProtocol of (string * IActorRef [])
    | InitializePushSum of Double * IActorRef []
    | PushSum of Double * Double * Double * IActorRef []
    | SetTopAlgo of int * IActorRef [] * int


let available (actorArr, ele2) =
    Array.exists (fun ele -> (ele = ele2)) actorArr

let WorkerActor actorId bossBRef (mailbox: Actor<_>) =
    let mutable neighbors: IActorRef [] = [||]
    let mutable myRef: IActorRef = null
    let mutable idx: int = 0
    let mutable actorsCount = 0
    let mutable s = actorId |> float
    let mutable w = 1.0
    let mutable nRounds = 1

    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
            | PopNeighbors (index, reference, actorReference) ->
                idx <- index
                myRef <- reference
                neighbors <- actorReference

            | InitializeGossipProtocol (message, faultyNodes) ->

                if (not (available (faultyNodes, myRef))) then
                    if actorsCount < 10 then
                        // Active actor
                        actorsCount <- actorsCount + 1

                    if actorsCount = 10 then
                        bossBRef <! GossipProtocolFinished (idx)

                    let len = neighbors.Length
                    let mutable randomIdx = randomR.Next(0, len)

                    while randomIdx = idx
                          || available (faultyNodes, neighbors.[randomIdx]) do
                        randomIdx <- randomR.Next(0, len)

                    neighbors.[randomIdx]
                    <! InitializeGossipProtocol(message, faultyNodes)

            | InitializePushSum (minimumDiff, faultyNodes) ->
                let index = randomR.Next(0, neighbors.Length)
                s <- s / 2.0
                w <- w / 2.0

                neighbors.[index]
                <! PushSum(s, w, minimumDiff, faultyNodes)

            | PushSum (s1, w1, minimumDiff, faultyNodes) ->
                if (not (available (faultyNodes, myRef))) then
                    let receiveSum = s + s1
                    let receiveWeight = w + w1
                    let len = neighbors.Length
                    let mutable randomIdx = randomR.Next(0, len)

                    while available (faultyNodes, neighbors.[randomIdx]) do
                        randomIdx <- randomR.Next(0, len)

                    let diff =
                        (s / w - receiveSum / receiveWeight) |> abs

                    if (diff > minimumDiff) then
                        s <- (s + s1) / 2.0
                        w <- (w + w1) / 2.0
                        nRounds <- 1

                        neighbors.[randomIdx]
                        <! PushSum(s, w, minimumDiff, faultyNodes)
                    elif (diff < minimumDiff && nRounds = 3) then
                        bossBRef <! PushSumFinished
                    else
                        s <- (s + s1) / 2.0
                        w <- (w + w1) / 2.0
                        nRounds <- nRounds + 1

                        neighbors.[randomIdx]
                        <! PushSum(s, w, minimumDiff, faultyNodes)

            | _ -> ()

            return! loop ()
        }

    loop ()

let master (mailbox: Actor<_>) =
    let mutable nodesN = 0
    let mutable countNodes = 0
    let mutable gSize = 0
    let mutable allNodes: IActorRef [] = [||]

    let rec loop () =
        actor {
            let! message = mailbox.Receive()

            match message with
            | SetTopAlgo (n, allActors, gridS) ->
                nodesN <- n
                gSize <- gridS
                allNodes <- allActors

            | PushSumFinished ->
                printfn "Convergence time for push sum is %d ms" tmr.ElapsedMilliseconds
                Environment.Exit 0

            | GossipProtocolFinished  actorId ->
                if not (actorsFinished.Contains(actorId)) then
                    countNodes <- countNodes + 1
                    actorsFinished.Add(actorId)
                    

                if countNodes = (nodesN - failureNodes) then
                    printfn "Convergence time for gossip is %d ms" tmr.ElapsedMilliseconds
                    Environment.Exit 0
            | _ -> ()

            return! loop ()
        }

    loop ()
// step 2
let startProtocol (algo: string, noOfNodes: int, actorsArr: IActorRef []) =
    let mutable faultyNodes = [||]

    for initializer in [ 0 .. failureNodes ] do
        let mutable rnd_shutdown = Random().Next(0, noOfNodes)
        faultyNodes <- Array.append faultyNodes [| actorsArr.[rnd_shutdown] |]


    let randomStart = randomR.Next(0, noOfNodes - 1)
    tmr.Start()

    if algo = "gossip" then
        printfn "Initiating Gossip"

        actorsArr.[randomStart]
        <! InitializeGossipProtocol("message", faultyNodes)
    else
        printfn "Initiating PushSum"

        actorsArr.[randomStart]
        <! InitializePushSum(10.0 ** -10.0, faultyNodes)


// First Step to start protocol
let sNtwrkTopo (noOfNodes: int, topologyT: string, algo: string, gSize: int) =
    let bossB = spawn system "Boss" master

    let workerActorsArr =
        (Array.init
            noOfNodes
            (fun index ->
                WorkerActor(index + 1) bossB
                |> spawn system ("Node" + string (index + 1))))

    bossB
    <! SetTopAlgo(noOfNodes, workerActorsArr, gSize)

    let get3dNeighbors(actorId: string, gridDict: Map<string, int>, actAddressDict: Map<string, int>) =

        let mutable res = [||]

        let a = ((actorId.[0] |> int) - 48)
        let b = ((actorId.[1] |> int) - 48)
        let c = ((actorId.[2] |> int) - 48)
        // for top neighbor
        let keyValue = string (a+ 1) + string (b) + string (c)

        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [| keyValue |]

        // for bottom neighbor
        let keyValue = string (a- 1) + string (b) + string (c)

        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [| keyValue |]

        // for left neighbor
        let keyValue = string (a) + string (b + 1) + string (c)

        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [| keyValue |]

        // for right neighbor
        let keyValue = string (a) + string (b - 1) + string (c)

        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [| keyValue |]

        // for front neighbor
        let keyValue = string (a) + string (b) + string (c + 1)

        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [| keyValue |]

        // for back neighbor
        let keyValue = string (a) + string (b) + string (c - 1)

        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [| keyValue |]

        let a = Array.map (fun id -> workerActorsArr.[actAddressDict.[id]]) res

        a


    match topologyT with
    | "full" ->
        for initializer in [ 0 .. noOfNodes - 1 ] do
            workerActorsArr.[initializer]
            <! PopNeighbors(initializer + 1, workerActorsArr.[initializer], workerActorsArr)

        startProtocol (algo, noOfNodes, workerActorsArr)

    | "line" ->
        for initializer in [ 0 .. noOfNodes - 1 ] do
            let mutable neighborArr: IActorRef [] = [||]

            if initializer = 0 then
                neighborArr <- [| workerActorsArr.[initializer + 1] |]
            elif initializer = noOfNodes - 1 then
                neighborArr <- [| workerActorsArr.[initializer - 1] |]
            else
                neighborArr <-
                    [| workerActorsArr.[initializer - 1]
                       workerActorsArr.[initializer + 1] |]

            workerActorsArr.[initializer]
            <! PopNeighbors(initializer + 1, workerActorsArr.[initializer], neighborArr)

        startProtocol (algo, noOfNodes, workerActorsArr)

    | "3D" ->
        let size =
            ceil ((float (noOfNodes) ** (1.0 / 3.0))) |> int

        let mutable nodesNAdded = 0
        let mutable gridDict = Map.empty
        let mutable actAddressDict = Map.empty

        for a in [ 0 .. size - 1 ] do
            for b in [ 0 .. size - 1 ] do
                for c in [ 0 .. size - 1 ] do
                    if (nodesNAdded < noOfNodes) then
                        let keyValue = string (a) + string (b) + string (c)
                        gridDict <- gridDict.Add(keyValue, 1)
                        actAddressDict <- actAddressDict.Add(keyValue, nodesNAdded)
                        nodesNAdded <- nodesNAdded + 1
                    else
                        gridDict <- gridDict.Add((string (a) + string (b) + string (c)), 0)

        
        for entryE in actAddressDict do
            if gridDict.[entryE.Key] = 1 then
                workerActorsArr.[entryE.Value]
                <! PopNeighbors(
                    (entryE.Value + 1),
                    workerActorsArr.[entryE.Value],
                    (get3dNeighbors(entryE.Key, gridDict, actAddressDict))
                )

        printfn "-------------"

        startProtocol (algo, noOfNodes, workerActorsArr)

    | "imp3D" ->
        let size =
            ceil ((float (noOfNodes) ** (1.0 / 3.0))) |> int

        let mutable nodesNAdded = 0
        let mutable gridDict = Map.empty
        let mutable actAddressDict = Map.empty

        for a in [ 0 .. size - 1 ] do
            for b in [ 0 .. size - 1 ] do
                for c in [ 0 .. size - 1 ] do
                    if (nodesNAdded < noOfNodes) then
                        let keyValue = string (a) + string (b) + string (c)
                        gridDict <- gridDict.Add(keyValue, 1)
                        actAddressDict <- actAddressDict.Add(keyValue, nodesNAdded)
                        nodesNAdded <- nodesNAdded + 1
                    else
                        gridDict <- gridDict.Add((string (a) + string (b) + string (c)), 0)


        for entryE in actAddressDict do
            if gridDict.[entryE.Key] = 1 then
                let mutable nArray =
                    (get3dNeighbors(entryE.Key, gridDict, actAddressDict))

                let mutable randomNeighbor = randomR.Next(0, noOfNodes - 1)

                while available (nArray, workerActorsArr.[randomNeighbor]) do
                    randomNeighbor <- randomR.Next(0, noOfNodes - 1)

                nArray <- Array.append nArray [| workerActorsArr.[randomNeighbor] |]
                // printfn "%A" nArray

                workerActorsArr.[entryE.Value]
                <! PopNeighbors((entryE.Value + 1), workerActorsArr.[entryE.Value], nArray)

        printfn "------------"

   

        startProtocol (algo, noOfNodes, workerActorsArr)


    | _ -> ()


sNtwrkTopo (noOfNodes, topologyT, algo, gSize)
System.Console.ReadLine() |> ignore