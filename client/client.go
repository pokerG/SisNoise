// Package client contains the functionality to run the client in SisNoise
package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	. "github.com/pokerG/SisNoise/common"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Config Options
var serverhost string                // serverhost
var serverport string                // serverport
var SIZEOFBLOCK int                  //size of block in bytes
var id string                        // the namenode id
var backups int                      // the number of back-up
var state = HB                       // internal statemachine
var sendChannel chan Packet          // for outbound Packets
var receiveChannel chan Packet       // for in bound Packets
var sendMap map[string]*json.Encoder // maps DatanodeIDs to their connections
var sendMapLock sync.Mutex

var encoder *json.Encoder
var decoder *json.Decoder

// SendPackets encodes packets and transmits them to their proper recipients
func SendPackets(encoder *json.Encoder, ch chan Packet) {
	for p := range ch {
		err := encoder.Encode(p)
		if err != nil {
			log.Println("error sending", p.DST)
		}
	}
}

// SendHeartbeat is used to notify the namenode of a valid connection
// on a periodic basis
func SendHeartbeat() {
	p := new(Packet)
	p.SRC = id
	p.DST = "NN"
	p.CMD = HB

	encoder.Encode(*p)
}

// BlocksHeadersFromFile generates Blockheaders without datanodeID assignments
// The client uses these headers to write blocks to datanodes
func BlockHeadersFromFile(localname, remotename string) []BlockHeader {

	var headers []BlockHeader

	info, err := os.Lstat(localname)
	if err != nil {
		panic(err)
	}

	// get read buffer
	fi, err := os.Open(localname)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	// Create Blocks
	numblocks := int(info.Size()/int64(SIZEOFBLOCK) + 1)
	headers = make([]BlockHeader, numblocks, numblocks)
	blocknum := 0

	for blocknum < numblocks {
		if strings.Index(remotename, "/") != 0 {
			remotename = "/" + remotename
		}

		n := SIZEOFBLOCK
		if blocknum == (numblocks - 1) {
			n = int(info.Size() % int64(SIZEOFBLOCK))

		}

		h := BlockHeader{"", remotename, n, blocknum, numblocks, 0}

		// load balance via roundrobin
		blocknum++
		headers[blocknum] = h
	}

	return headers
}

// WriteJSON writes a JSON encoded interface to disc
func WriteJSON(fileName string, key interface{}) {
	outFile, err := os.Create(fileName)
	defer outFile.Close()
	if err != nil {
		log.Println("Error opening JSON ", err)
		return
	}
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	if err != nil {
		log.Println("Error encoding JSON ", err)
		return
	}
}

// BlocksFromFile split a File into Blocks for storage on the filesystem
// in the future this will read a fixed number of blocks at a time from disc for reasonable memory utilization
func DistributeBlocksFromFile(localname, remotename string) error {

	info, err := os.Lstat(localname)
	if err != nil {
		return err
	}

	// get read buffer
	fi, err := os.Open(localname)
	if err != nil {
		return err
	}
	r := bufio.NewReader(fi)

	// Create Blocks
	total := int((info.Size() / int64(SIZEOFBLOCK)) + 1)

	num := 0

	for num < total {

		// read a chunk from file into Block data buffer
		buf := make([]byte, SIZEOFBLOCK)
		w := bytes.NewBuffer(nil)

		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}

		// write full Block to disc
		if strings.Index(remotename, "/") != 0 {
			remotename = "/" + remotename
		}

		h := BlockHeader{"", remotename, n, num, total, 0}

		data := make([]byte, 0, n)
		data = w.Bytes()[0:n]
		b := Block{h, data}

		err = DistributeBlock(b)
		if err != nil {
			return err
		}

		fmt.Printf(".")
		// generate new Block
		num += 1

	}
	if err := fi.Close(); err != nil {
		return err
	}

	fmt.Printf(" Done! \n")
	return nil
}

func DistributeBlock(b Block) error {

	p := new(Packet)
	p.SRC = id
	p.DST = "NN"
	p.CMD = DISTRIBUTE
	p.Data = b
	encoder.Encode(*p)

	var r Packet
	decoder.Decode(&r)
	if r.CMD != ACK {
		return errors.New("Could not distribute block to namenode")
	}
	return nil
}

func DistributeBlocks(blocks []Block) error {

	fmt.Println("Distributing file blocks")
	for _, b := range blocks {

		err := DistributeBlock(b)
		if err != nil {
			return errors.New("Distrubution Error: " + err.Error())
		}
		fmt.Printf(".")
	}
	fmt.Printf(" Done! \n")

	return nil
}

//.................................
// RemoveFile delete a file or directory from the remote file system.
func RemoveFile(remotename string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to remove file")
			return
		}
	}()
	// send header request to check up whether the header exist.
	p := new(Packet)
	p.DST = "NN"
	p.SRC = id
	p.CMD = GETHEADERS
	p.Headers = make([]BlockHeader, 1, 1)
	p.Headers[0] = BlockHeader{"", remotename, 0, 0, 0, 0}
	encoder.Encode(*p)

	// get header list
	var r Packet
	decoder.Decode(&r)

	if r.CMD == ERROR {
		fmt.Println(r.Message)
		return
	}
	if r.CMD != GETHEADERS || r.Headers == nil {
		if r.CMD == ERROR {
			fmt.Println(r.Message)
		} else {
			fmt.Println("Bad response packet ", r)
		}
		return
	}
	headers := r.Headers
	h := headers[0]
	q := new(Packet)
	q.DST = "NN"
	q.SRC = id
	q.CMD = DELETE
	q.Headers = make([]BlockHeader, 1, 1)
	q.Headers[0] = h
	encoder.Encode(*q)

	var rr Packet
	decoder.Decode(&rr)

	if rr.CMD == ERROR {
		fmt.Println(r.Message)
		return
	}
	if rr.CMD != DELETE || rr.Headers == nil {
		if rr.CMD == ERROR {
			fmt.Println(rr.Message)
		} else {
			fmt.Println("Bad response packet ", r)
		}
		return
	}
	fmt.Println("Done!")
}

//.................................

// RetrieveFile queries the filesystem for the File located at remotename,
// and saves its contents to the file localname
func RetrieveFile(localname, remotename string) {
	// TODO make this handle errors gracefully
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to retrieve file")
			return
		}
	}()
	// send header request
	p := new(Packet)
	p.DST = "NN"
	p.SRC = id
	p.CMD = GETHEADERS
	p.Headers = make([]BlockHeader, 1, 1)
	p.Headers[0] = BlockHeader{"", remotename, 0, 0, 0, 0}
	encoder.Encode(*p)

	// get header list
	var r Packet
	decoder.Decode(&r)

	if r.CMD == ERROR {
		fmt.Println(r.Message)
		return
	}

	if r.CMD != GETHEADERS || r.Headers == nil {
		if r.CMD == ERROR {
			fmt.Println(r.Message)
		} else {
			fmt.Println("Bad response packet ", r)
		}
		return
	}

	// setup writer
	paths := strings.Split(localname, "/")
	dir := ""
	for k, v := range paths {
		if k != len(paths)-1 {
			dir += "/" + v
		}
	}
	dir = strings.TrimPrefix(dir, "/")
	os.MkdirAll(dir, 0777)
	outFile, err := os.Create(localname)
	if err != nil {
		fmt.Println("error constructing file: ", err)
		return
	}
	defer outFile.Close()
	w := bufio.NewWriterSize(outFile, SIZEOFBLOCK)

	// for each header, retrieve its block and write to disc
	headers := r.Headers

	fmt.Println("Received File Headers for ", p.Headers[0].Filename, ". Retrieving ", r.Headers[0].NumBlocks, " Blocks ")

	for _, h := range headers {

		// send request
		p := new(Packet)
		p.DST = "NN"
		p.SRC = id
		p.CMD = RETRIEVEBLOCK
		p.Headers = make([]BlockHeader, 1, 1)
		p.Headers[0] = h

		encoder.Encode(*p)

		// receive block
		var r Packet
		decoder.Decode(&r)

		if r.CMD != BLOCK {
			if r.CMD == ERROR {
				fmt.Println(r.Message)
			} else {
				fmt.Println("Bad response packet ", r)
			}
			return
		}
		b := r.Data
		n := b.Header.Size

		_, err := w.Write(b.Data[:n])
		if err != nil {
			panic(err)
		}
		fmt.Printf(".")
		w.Flush()
	}

	fmt.Printf(" Done! \n")
	fmt.Println("Wrote file to disc at ", localname)
}

// ReceiveInput provides user interaction and file placement/retrieval from remote filesystem
func ReceiveInput() {
	fmt.Printf("Valid Commands: \n \t put [-r] localinput remoteoutput \n \t get [-r] remoteinput localoutput \n \t rm [-r] remotepath \n \t list path\n")
	for {
		fmt.Printf(">>> ")
		scanner := bufio.NewScanner(os.Stdin)
		var cmd string
		scanner.Scan()
		lns := strings.Split(strings.TrimSpace(scanner.Text()), " ")
		cmd = lns[0]
		if !(cmd == "put" || cmd == "get" || cmd == "list" || cmd == "rm") {
			fmt.Printf("Incorrect command\n Valid Commands: \n \t put [-r] localinput remoteoutput \n \t get [-r] remoteinput localoutput \n \t rm [-r] remotepath \n \t list path\n")
			continue
		}

		switch cmd {

		//............................................
		case "rm":
			if lns[1] == "-r" {
				remotecatalogue := strings.TrimSuffix(lns[2], "/")
				fmt.Println("Retmove files")
				files := strings.Split(RetrieveDir(remotecatalogue), "#")
				fmt.Println("Files Number: ", len(files))
				for _, v := range files {
					if v == "" {
						continue
					}
					remotename := ""
					tmpfile := strings.Split(v, "/")
					for _, vv := range tmpfile {
						if vv != "" {
							remotename = remotename + "#" + vv
						}
					}
					remotename = "/" + remotename

					RemoveFile(remotename)
				}
			} else {
				tmpfile := strings.Split(lns[1], "/")
				remotename := ""
				for _, v := range tmpfile {
					if v != "" {
						remotename = remotename + "#" + v
					}
				}
				remotename = "/" + remotename
				fmt.Println("Remove file")
				RemoveFile(remotename)
			}
			//.....................................

		case "put":
			if lns[1] == "-r" {
				localcatalogue := lns[2]
				tmpfile := strings.Split(lns[3], "/")
				remotecatalogue := ""
				for _, v := range tmpfile {
					if v != "" {
						remotecatalogue = remotecatalogue + "#" + v
					}

				}
				err := putDir(localcatalogue, remotecatalogue)
				if err != nil {
					fmt.Println(err)
					continue
				}
			} else {
				localname := lns[1]
				tmpfile := strings.Split(lns[2], "/")
				remotename := ""
				for _, v := range tmpfile {
					if v != "" {
						remotename = remotename + "#" + v
					}

				}

				_, err := os.Lstat(localname)
				if err != nil {
					fmt.Println("File ", localname, " could not be accessed")
					continue
				}

				// generate blocks from new File for distribution
				err = DistributeBlocksFromFile(localname, remotename)

				if err != nil {
					fmt.Println(err)
					continue
				}
			}

		case "get":
			if lns[1] == "-r" {
				localcatalogue := strings.TrimSuffix(lns[3], "/")
				remotecatalogue := strings.TrimSuffix(lns[2], "/")
				fmt.Println("Retrieving files")
				files := strings.Split(RetrieveDir(remotecatalogue), "#")
				fmt.Println("Files Number: ", len(files))
				for _, v := range files {
					if v == "" {
						continue
					}
					remotename := ""
					tmpfile := strings.Split(v, "/")
					for _, vv := range tmpfile {
						if vv != "" {
							remotename = remotename + "#" + vv
						}
					}
					remotename = "/" + remotename

					RetrieveFile(localcatalogue+v, remotename)
				}
			} else {
				tmpfile := strings.Split(lns[1], "/")
				remotename := ""
				for _, v := range tmpfile {
					if v != "" {
						remotename = remotename + "#" + v
					}
				}
				remotename = "/" + remotename
				localname := lns[2]
				fmt.Println("Retrieving file")
				RetrieveFile(localname, remotename)
			}

		case "list":
			if len(lns) == 1 {
				RetrieveList("/")
			} else {
				RetrieveList(lns[1])
			}
			fmt.Println("Retrieving List ")

		}
	}

}

// RetrieveDir gets a file listing from the namenode
func RetrieveDir(path string) string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to retrieve file")
			return
		}
	}()

	// send header request
	p := new(Packet)
	p.DST = "NN"
	p.SRC = id
	p.CMD = DIR
	p.Message = path

	encoder.Encode(*p)

	// get header list
	var r Packet
	decoder.Decode(&r)

	if r.CMD == ERROR {
		fmt.Println(r.Message)
		return ""
	}

	if r.CMD != DIR {
		fmt.Println("Bad response packet ", r)
		return ""
	}

	return r.Message
}

// RetrieveList gets a file listing from the namenode
func RetrieveList(path string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to retrieve file")
			return
		}
	}()

	// send header request
	p := new(Packet)
	p.DST = "NN"
	p.SRC = id
	p.CMD = LIST
	p.Message = path

	encoder.Encode(*p)

	// get header list
	var r Packet
	decoder.Decode(&r)

	if r.CMD == ERROR {
		fmt.Println(r.Message)
		return
	}

	if r.CMD != LIST {
		fmt.Println("Bad response packet ", r)
		return
	}

	fmt.Println(r.Message)

}

// Parse Config sets up the node with the provided XML file
func ParseConfigXML(configpath string) error {
	xmlFile, err := os.Open(configpath)
	if err != nil {
		return err
	}
	defer xmlFile.Close()

	var list ConfigOptionList
	err = xml.NewDecoder(xmlFile).Decode(&list)
	if err != nil {
		return err
	}

	for _, o := range list.ConfigOptions {

		switch o.Key {
		// single client for now
		//	case "id":
		//		id = o.Value
		case "serverhost":
			serverhost = o.Value
		case "serverport":
			serverport = o.Value
		case "sizeofblock":
			n, err := strconv.Atoi(o.Value)
			if err != nil {
				return err
			}

			if n < 4096 {
				return errors.New("Buffer size must be greater than or equal to 4096 bytes")
			}
			SIZEOFBLOCK = n
		default:
			return errors.New("Bad ConfigOption received Key : " + o.Key + " Value : " + o.Value)
		}
	}

	return nil
}

// Initializes the client and begins communication
func Run(configpath string) {

	ParseConfigXML(configpath)

	id = "C"
	conn, err := net.Dial("tcp", serverhost+":"+serverport)
	CheckError(err)

	encoder = json.NewEncoder(conn)
	decoder = json.NewDecoder(conn)

	// Start communication
	//	SendHeartbeat()
	ReceiveInput()

	os.Exit(0)
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}

// put files in catalogue recursively
func putDir(local string, remote string) error {
	dirAbs, err := filepath.Abs(local)
	if err != nil {
		fmt.Println("111")
		return err
	}
	fileInfos, err := ioutil.ReadDir(dirAbs)
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			err = putDir(filepath.Join(dirAbs, fileInfo.Name()), remote+"#"+fileInfo.Name())
			if err != nil {
				fmt.Println("222")
				return err
			}
		} else {
			err = DistributeBlocksFromFile(filepath.Join(dirAbs, fileInfo.Name()), remote+"#"+fileInfo.Name())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
