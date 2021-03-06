// Package datanode contains the functionality to run a datanode in SisNoise
package datanode

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	. "github.com/pokerG/SisNoise/common"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config Options
var serverhost string // server host
var serverport string // server port
var SIZEOFBLOCK int64 // size of block in bytes
var id string         // the datanode id
var root string       // the root location on disk to store Blocks

var state = HB // internal statemachine

// ReceivePacket decodes a packet and adds it to the handler channel
// for processing by the datanode
func ReceivePackets(decoder *json.Decoder, p chan Packet) {
	for {
		r := new(Packet)
		decoder.Decode(r)
		p <- *r
	}
}

// SendHeartbeat is used to notify the namenode of a valid connection
// on a periodic basis
func SendHeartbeat(encoder *json.Encoder) {
	p := new(Packet)
	p.SRC = id
	p.DST = "NN"
	p.CMD = HB
	encoder.Encode(p)
}

// HandleResponse delegates actions to perform based on the
// contents of a recieved Packet, and encodes a response
func HandleResponse(p Packet, encoder *json.Encoder) {
	r := new(Packet)
	r.SRC = id
	r.DST = p.SRC

	switch p.CMD {
	case ACK:
		return
	case LIST:
		list := GetBlockHeaders()
		r.Headers = make([]BlockHeader, len(list))

		for i, b := range list {
			r.Headers[i] = b
		}
		r.CMD = LIST
	case BLOCK:
		r.CMD = BLOCKACK
		WriteBlock(p.Data)
		r.Headers = make([]BlockHeader, 0, 2)
		r.Headers = append(r.Headers, p.Data.Header)
	case DELETE:
		r.CMD = DELETE
		headers := p.Headers
		bh := headers[0]
		filename := root + bh.Filename
		err := os.RemoveAll(filename)
		if err != nil {
			fmt.Println("Error delete file! ", err)
			return
		}
	case RETRIEVEBLOCK:
		fmt.Println("retrieving block from ", p.Headers[0])
		b := BlockFromHeader(p.Headers[0])
		r.CMD = BLOCK
		r.Data = b
	}
	encoder.Encode(*r)
}

// WriteBlock performs all functionality necessary to write a Block b
// to the local filesystem
func WriteBlock(b Block) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to write Block")
			return
		}
	}()

	list, err := ioutil.ReadDir(root)
	h := b.Header
	tmf := strings.Split(h.Filename, "/")
	Filename := "/" + tmf[len(tmf)-1]
	fname := Filename + "/" + strconv.Itoa(h.BlockNum)

	for _, dir := range list {

		if "/"+dir.Name() == Filename {
			WriteJSON(root+fname, b)
			log.Println("Wrote Block ", root+fname, "to disc")
			return
		}
	}
	// create directory
	err = os.Mkdir(root+Filename, 0700)
	if err != nil {
		fmt.Println("path error : ", err)
		return
	}

	fname = Filename + "/" + strconv.Itoa(h.BlockNum)
	WriteJSON(root+fname, b)
	log.Println("Wrote Block ", root+fname, "to disc")
	return

}

// GetBlockHeaders retrieves the list of all Blockheaders found within
// the filesystem specified by the user.
func GetBlockHeaders() []BlockHeader {

	list, err := ioutil.ReadDir(root)

	CheckError(err)
	headers := make([]BlockHeader, 0, len(list))

	// each directory is Filename, which holds Block files within
	for _, dir := range list {

		files, err := ioutil.ReadDir(dir.Name())
		if err != nil {
			log.Println("Error reading directory ", err)
		} else {
			for _, fi := range files {
				var b Block
				fpath := strings.Join([]string{root, dir.Name(), fi.Name()}, "/")
				ReadJSON(fpath, &b)
				headers = append(headers, b.Header)
			}
		}
	}
	return headers
}

// BlockFromHeader retrieves a Block using metadata from the Blockheader h
func BlockFromHeader(h BlockHeader) Block {

	list, err := ioutil.ReadDir(root)
	var errBlock Block
	if err != nil {
		log.Println("Error reading directory ", err)
		return errBlock
	}

	fname := h.Filename + "/" + strconv.Itoa(h.BlockNum)
	for _, dir := range list {
		var b Block
		if "/"+dir.Name() == h.Filename {
			ReadJSON(root+fname, &b)
			return b
		}
	}
	fmt.Println("Block not found ", root+fname)
	return errBlock
}

// ReadJSON reads a JSON encoded interface to disc
func ReadJSON(fname string, key interface{}) {
	fi, err := os.Open(fname)
	defer fi.Close()

	if err != nil {
		fmt.Println("Couldnt Read JSON ")
		return
	}

	decoder := json.NewDecoder(fi)
	err = decoder.Decode(key)
	if err != nil {
		log.Println("Error encoding JSON ", err)
		return
	}
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
		case "id":
			id = o.Value
		case "blockfileroot":
			root = o.Value
		case "serverhost":
			serverhost = o.Value
		case "serverport":
			serverport = o.Value
		case "sizeofblock":
			n, err := strconv.ParseInt(o.Value, 0, 64)
			if err != nil {
				return err
			}

			if n < int64(4096) {
				return errors.New("Buffer size must be greater than or equal to 4096 bytes")
			}
			SIZEOFBLOCK = n
		default:
			return errors.New("Bad ConfigOption received Key : " + o.Key + " Value : " + o.Value)
		}
	}

	return nil
}

func Run(configpath string) {

	ParseConfigXML(configpath)

	err := os.Chdir(root)
	CheckError(err)

	conn, err := net.Dial("tcp", serverhost+":"+serverport)
	CheckError(err)

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	PacketChannel := make(chan Packet)
	// start communication
	go ReceivePackets(decoder, PacketChannel)
	tick := time.Tick(2 * time.Second)
	for {
		select {
		case <-tick:
			SendHeartbeat(encoder)
		case r := <-PacketChannel:
			HandleResponse(r, encoder)
		}

	}
	os.Exit(0)
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
