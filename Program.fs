open Elmish
open System
open System.IO
open System.Threading

type Model = { Lines: uint; Volume: decimal }

type Trade =
    { Time: DateTimeOffset
      Price: decimal
      Quantity: decimal
      IsBuyerMaker: bool }

type Msg =
    | Trade of Trade
    | CsvLoaded

let dateTimeOffsetOfBase64 (base64: string) =
    let base64Padding = String.replicate ((4 - (base64.Length % 4)) % 4) "="
    let bytes = base64 + base64Padding |> Convert.FromBase64String
    let bytePadding = Array.replicate (8 - bytes.Length) 0uy

    Array.append bytePadding bytes
    |> Array.rev
    |> BitConverter.ToInt64
    |> DateTimeOffset.FromUnixTimeMilliseconds

let tradeOfCsvLine (line: string) =
    match line.Split ',' |> List.ofArray with
    | [ date; price; quantity; isBuyerMaker ] ->
        { Time = dateTimeOffsetOfBase64 date
          Price = decimal price
          Quantity = decimal quantity
          IsBuyerMaker = if byte isBuyerMaker = 1uy then true else false }
    | list -> failwithf "Bad line in CSV: %A" list

let csvFileName = "big.csv"
let csvReader = new StreamReader(csvFileName)

let readCsvHeader () =
    task {
        if csvReader.EndOfStream then
            return failwith $"No header in CSV file {csvFileName}"
        else
            let! line = csvReader.ReadLineAsync()

            return
                match line.Split ',' |> List.ofArray with
                | [ "Time"; "Price"; "Quantity"; "IsBuyerMaker" ] -> ()
                | line -> failwithf "Bad header in CSV file: %A" line
    }

let readCsvTrade () =
    task {
        if csvReader.EndOfStream then
            return CsvLoaded
        else
            let! line = csvReader.ReadLineAsync()
            return tradeOfCsvLine line |> Trade
    }

let init () =
    { Lines = 0u; Volume = 0m },
    Cmd.OfTask.perform
        (fun () ->
            task {
                do! readCsvHeader ()
                return! readCsvTrade ()
            })
        ()
        id

let update msg model =
    match msg with
    | Trade trade ->
        { Lines = model.Lines + 1u
          Volume = model.Volume + trade.Quantity },
        Cmd.OfTask.perform readCsvTrade () id
    | CsvLoaded -> model, []

let view model _ =
    printfn "%A: %A" model.Lines model.Volume

let completionHandle = new ManualResetEvent(false)

Program.mkProgram init update view
|> Program.withTermination ((=) CsvLoaded) (fun _ -> completionHandle.Set() |> ignore)
|> Program.run

completionHandle.WaitOne() |> ignore
exit 0
