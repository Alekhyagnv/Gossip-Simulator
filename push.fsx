#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open System.Collections.Generic
open Akka.Actor
open Akka.FSharp
open System.Diagnostics
open Akka.Configuration

let args : string array = fsi.CommandLineArgs |> Array.tail
let algo = args.[2]
let mutable gSize =0
let system = ActorSystem.Create("topologyTSystem")
let tmr = Stopwatch()
let topologyT = args.[1]
let mutable workerActArr = [||] 
let randomR  = System.Random()
let mutable actorsFinished = new List<int>()
let mutable noOfNodes = args.[0] |> int


type Simulator =
    | PushSumFinished
    | GossipProtocolFinished of (int)
    | PopNeighbors of (int* IActorRef * IActorRef[])
    | InitializeGossipProtocol of (string)
    | InitializePushSum of Double
    | PushSum of Double*Double*Double
    | SetTopAlgo of int * IActorRef[]  * int


// next step
let initializeProtocol (algo: string, noOfNodes: int, actorsArr: IActorRef []) =
    let mutable randomStart = randomR.Next(0, noOfNodes - 1)
    tmr.Start()

    if algo = "gossip" then
        printfn "Initiating Gossip"
        actorsArr.[randomStart] <! InitializeGossipProtocol("message")
        

    else
        printfn "Initiating PushSum"
        actorsArr.[randomStart]
        <! InitializePushSum(10.0 ** -10.0)

let master (mailbox: Actor<_>) =
    let mutable nodesN = 0
    let mutable countNodes = 0
    let mutable gSize = 0
    let mutable allNodes:IActorRef[] = [||]
    let mutable s = Set.empty

    let rec loop() = actor {
        let! message = mailbox.Receive()

        match message with 
        | SetTopAlgo (n,allActors,gridS) -> 
            nodesN <- n
            gSize <- gridS
            allNodes <- allActors

        | PushSumFinished -> 
            printfn "Convergence time in Push-Sum is %d ms" tmr.ElapsedMilliseconds
            Environment.Exit 0

        | GossipProtocolFinished actorId -> 
            if not(actorsFinished.Contains(actorId)) then
                countNodes <- countNodes + 1
                actorsFinished.Add(actorId)

            if countNodes = nodesN then
               printfn "Convergence time in Gossip is %d ms" tmr.ElapsedMilliseconds
               Environment.Exit 0
        |_ -> ()        

        return! loop()
    }
    loop()



let WorkerActor actorId bossRef (mailbox: Actor<_>) = 
    let mutable neighbors:IActorRef[]=[||]
    let mutable myRef:IActorRef = null
    let mutable idx:int = 0
    let mutable actorsCount =0
    let mutable s = actorId |> float
    let mutable w = 1.0
    let mutable nRounds = 1

    let rec loop() =  actor {
        let! message = mailbox.Receive()
        match message with
        | PopNeighbors(index, reference, actorReference) -> 
            idx <- index 
            myRef <- reference
            neighbors <- actorReference

        | InitializeGossipProtocol (message) -> 
            if actorsCount < 10 then 
                actorsCount <- actorsCount + 1 
    
            if actorsCount = 10 then
                bossRef <! GossipProtocolFinished(idx) 

            let len = neighbors.Length
            let mutable randomIdx = randomR.Next(0, len)

            while randomIdx = idx do
                randomIdx <- randomR.Next(0, len)

            neighbors.[randomIdx]
            <! InitializeGossipProtocol(message)  

        | InitializePushSum (minimumDiff) ->
            let index= randomR.Next(0,neighbors.Length)
            s<- s/2.0
            w <-w/2.0
            neighbors.[index] <! PushSum(s,w,minimumDiff)
        
        | PushSum (s1,w1,minimumDiff) -> 
            
           let receiveSum = s + s1
           let receiveWeight = w + w1
           let len = neighbors.Length
           let randomIdx = randomR.Next(0,len)
           let diff =  (s/w - receiveSum/receiveWeight) |> abs
           
           if (diff > minimumDiff) then   
               s <- (s+s1)/2.0
               w <- (w+w1)/2.0
               nRounds <- 1
               neighbors.[randomIdx] <! PushSum(s,w,minimumDiff)
            elif (diff < minimumDiff && nRounds = 3) then
               bossRef <! PushSumFinished
            else
               s <- (s+s1)/2.0
               w <- (w+w1)/2.0
               nRounds <- nRounds+1
               neighbors.[randomIdx] <! PushSum(s,w,minimumDiff)    

        |_ -> ()
        
        return! loop()
    }
    loop()



// First Step to start protocol
let sNtwrkTopo(noOfNodes: int, topologyT: string, algo: string, gSize: int) =
    let bossB = spawn system "Boss" master
    workerActArr <- (Array.init noOfNodes (fun index -> WorkerActor (index+1) bossB |> spawn system ("Node"+string(index+1))))

    bossB <! SetTopAlgo(noOfNodes,workerActArr,gSize)

    let get3dNeighbors(actorId: string, gridDict: Map<string, int>, actAddressDict: Map<string, int>) =
        let mutable res = [||]
        let arr = actorId.Split("-")

        let a = ((arr.[0] |> int))
        let b = ((arr.[1] |> int))
        let c = ((arr.[2] |> int))

        // For top neighbor
        let keyValue= string(a+1)+"-"+string(b)+"-"+string(c)
        if gridDict.ContainsKey(keyValue) &&  gridDict.[keyValue] = 1 then
            res <- Array.append res [|keyValue|]

        // For bottom neighbor    
        let keyValue= string(a-1)+"-"+string(b)+"-"+string(c)
        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [|keyValue|]
        
        // For left neighbor
        let keyValue= string(a)+"-"+string(b+1)+"-"+string(c)
        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [|keyValue|]

        // For right neighbor
        let keyValue= string(a)+"-"+string(b-1)+"-"+string(c)
        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [|keyValue|]

        // For front neighbor
        let keyValue= string(a)+"-"+string(b)+"-"+string(c+1)
        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [|keyValue|]
        
        // For back neighbor
        let keyValue= string(a)+"-"+string(b)+"-"+string(c-1)
        if gridDict.ContainsKey(keyValue) && gridDict.[keyValue] = 1 then
            res <- Array.append res [|keyValue|]

        let a = Array.map (fun id -> workerActArr.[actAddressDict.[id]]) res
        a


    match topologyT with
    | "full" -> 
        for initializer in [0 .. noOfNodes-1] do
            workerActArr.[initializer] <! PopNeighbors(initializer+1,workerActArr.[initializer],workerActArr)
        initializeProtocol(algo,noOfNodes,workerActArr)
     | "line" ->
         for initializer in [0 .. noOfNodes-1] do
            let mutable neighborArr:IActorRef[] = [||]
            if initializer = 0 then 
               neighborArr <- [|workerActArr.[initializer+1]|] 
            elif initializer = noOfNodes-1 then
               neighborArr <- [|workerActArr.[initializer-1]|]
            else 
               neighborArr <- [|workerActArr.[initializer-1]; workerActArr.[initializer+1]|]
            workerActArr.[initializer] <! PopNeighbors(initializer+1,workerActArr.[initializer],neighborArr)  
         initializeProtocol(algo,noOfNodes,workerActArr)
    | "3D" ->
        let size = ceil((float (noOfNodes) ** (1.0/3.0))) |> int
        let mutable nodesAdded = 0
        let mutable gridDict = Map.empty
        let mutable actAddressDict = Map.empty

        for a in [0..size-1] do
            for b in [0..size-1] do
                for c in [0..size-1] do
                    if(nodesAdded < noOfNodes) then
                        let keyValue = string(a)+"-"+string(b)+"-"+string(c)
                        gridDict <- gridDict.Add(keyValue, 1)
                        actAddressDict <- actAddressDict.Add(keyValue, nodesAdded)
                        nodesAdded <- nodesAdded+1
                    else
                        gridDict <- gridDict.Add((string(a)+"-"+string(b)+"-"+string(c)), 0)

        

        for entryE in actAddressDict do
            if gridDict.[entryE.Key] = 1 then
                workerActArr.[entryE.Value] <! PopNeighbors((entryE.Value+1), workerActArr.[entryE.Value],(get3dNeighbors (entryE.Key, gridDict, actAddressDict)))
        
        printfn "----------------"

        
        initializeProtocol(algo, noOfNodes, workerActArr)
    
    | "imp3D" ->
        let size = ceil((float (noOfNodes) ** (1.0/3.0))) |> int
        let mutable nodesAdded = 0
        let mutable gridDict = Map.empty
        let mutable actAddressDict = Map.empty

        for a in [0..size-1] do
            for b in [0..size-1] do
                for c in [0..size-1] do
                    if(nodesAdded < noOfNodes) then
                        let keyValue = string(a)+"-"+string(b)+"-"+string(c)
                        gridDict <- gridDict.Add(keyValue, 1)
                        actAddressDict <- actAddressDict.Add(keyValue, nodesAdded)
                        nodesAdded <- nodesAdded+1
                    else
                        gridDict <- gridDict.Add((string(a)+"-"+string(b)+"-"+string(c)), 0)

        for entryE in actAddressDict do
            if gridDict.[entryE.Key] = 1 then
                let mutable nArray = (get3dNeighbors (entryE.Key, gridDict, actAddressDict))
                let mutable randomNeighbor = randomR.Next(0,noOfNodes-1)

                let isAvailable (actorArr, ele2) = Array.exists(fun ele -> ( ele = ele2)) actorArr

                while isAvailable(nArray, workerActArr.[randomNeighbor]) do
                    randomNeighbor <- randomR.Next(0,noOfNodes-1)   

                nArray <- Array.append nArray [|workerActArr.[randomNeighbor]|]
                
                workerActArr.[entryE.Value] <! PopNeighbors((entryE.Value+1), workerActArr.[entryE.Value], nArray)
        
        initializeProtocol(algo, noOfNodes, workerActArr)
            

    | _ -> ()




sNtwrkTopo(noOfNodes, topologyT, algo, gSize)       
System.Console.ReadLine() |> ignore