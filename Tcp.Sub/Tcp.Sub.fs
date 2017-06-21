module Tcp.Sub

open Tcp.Lib
open System
open System.Net
open System.Text
open System.Threading

[<EntryPoint>]
let main argv =
  PubSub.receiver()
  0
