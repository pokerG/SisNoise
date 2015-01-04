package main

import (
	"fmt"
	"github.com/pokerG/SisNoise/client"
	"github.com/pokerG/SisNoise/datanode"
	"github.com/pokerG/SisNoise/namenode"
	"os"
	"runtime"
)

func main() {

	if !(len(os.Args) == 3 && (os.Args[1] == "namenode" || os.Args[1] == "client" || os.Args[1] == "datanode")) {
		fmt.Println("Invalid command, usage : ")
		fmt.Println(" \t SisNoise namenode [location of namenode configuration file] ")
		fmt.Println(" \t SisNoise datanode [location of datanode configuration file] ")
		fmt.Println(" \t SisNoise client [location of client configuration file] ")

		os.Exit(1)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	cmd := os.Args[1]
	configpath := os.Args[2]

	switch cmd {
	case "namenode":
		namenode.Run(configpath)
	case "datanode":
		datanode.Run(configpath)
	case "client":
		fmt.Println(configpath)
		client.Run(configpath)
	}

}
