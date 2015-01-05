// Package datanode contains the functionality to run a namenode in SisNoise
package namenode

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	simplejson "github.com/bitly/go-simplejson"
	. "github.com/pokerG/SisNoise/common"
)

// Config Options
var host string         // listen host
var port string         // listen port
var SIZEOFBLOCK int     //size of block in bytes
var id string           // the namenode id
var metadatapath string //the meta-data directory
var backups int         // the number of back-up

var headerChannel chan BlockHeader   // processes headers into filesystem
var sendChannel chan Packet          //  enqueued packets for transmission
var sendMap map[string]*json.Encoder // maps DatanodeIDs to their connections
var sendMapLock sync.Mutex
var clientMap map[BlockHeader]string // maps requested Blocks to the client ID which requested them, based on Blockheader
var clientMapLock sync.Mutex

var blockReceiverChannel chan Block        // used to fetch blocks on user request
var blockRequestorChannel chan BlockHeader // used to send block requests

var root *filenode                             // the filesystem
var filemap map[string](map[int][]BlockHeader) // filenames to blocknumbers to headers
var datanodemap map[string]*datanode           // filenames to datanodes
var fsTree *simplejson.Json                    //the File system tree

var consistent *Consistent

// filenodes compose an internal tree representation of the filesystem
type filenode struct {
	path     string
	parent   *filenode
	children []*filenode
}

// Represent connected Datanodes
// Hold file and connection information
type datanode struct {
	ID        string
	listed    bool
	size      int64
	connected bool
}

// By is used to select the fields used when comparing datanodes
type By func(p1, p2 *datanode) bool

// Sort sorts an array of datanodes by the function By
func (by By) Sort(nodes []datanode) {
	s := &datanodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(s)
}

// structure to sort datanodes
type datanodeSorter struct {
	nodes []datanode
	by    func(p1, p2 *datanode) bool // Closure used in the Less method.
}

func (s *datanodeSorter) Len() int {
	return len(s.nodes)
}
func (s *datanodeSorter) Swap(i, j int) {
	s.nodes[i], s.nodes[j] = s.nodes[j], s.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *datanodeSorter) Less(i, j int) bool {
	return s.by(&s.nodes[i], &s.nodes[j])
}

// Sendpacket abstracts packet sending details
func (dn *datanode) SendPacket(p Packet) {
	sendChannel <- p
}

// HandleBlockHeaders reads incoming BlockHeaders and merges them into the filesystem
func HandleBlockHeaders() {
	for h := range headerChannel {
		MergeNode(h)
	}
}

// ContainsHeader searches a BlockHeader for a given BlockHeader
func ContainsHeader(arr []BlockHeader, h BlockHeader) bool {
	for _, v := range arr {
		if h == v {
			return true
		}
	}
	return false
}

// Mergenode adds a BlockHeader entry to the filesystem, in its correct location
func MergeNode(h BlockHeader) error {

	if &h == nil || h.DatanodeID == "" || h.Filename == "" || h.Size < 0 || h.BlockNum < 0 || h.NumBlocks < h.BlockNum {
		return errors.New("Invalid header input")
	}

	dn, ok := datanodemap[h.DatanodeID]
	if !ok {
		return errors.New("BlockHeader DatanodeID: " + h.DatanodeID + " does not exist in map")
	}

	path := h.Filename
	path_arr := strings.Split(path, "/")
	q := root

	for i, _ := range path_arr {
		//skip
		if i == 0 {
			continue
		} else {

			partial := strings.Join(path_arr[0:i+1], "/")
			exists := false
			for _, v := range q.children {
				if v.path == partial {
					q = v
					exists = true
					break
				}
			}
			if exists {
				// If file already been added, we add the BlockHeader to the map
				if partial == path {
					blks, ok := filemap[path]
					if !ok {
						return errors.New("Attempted to add to filenode that does not exist!")
					}
					_, ok = blks[h.BlockNum]
					if !ok {
						filemap[partial][h.BlockNum] = make([]BlockHeader, 1, 1)
						filemap[partial][h.BlockNum][0] = h

					} else {
						if !ContainsHeader(filemap[path][h.BlockNum], h) {
							filemap[path][h.BlockNum] = append(filemap[path][h.BlockNum], h)

						}
					}
					dn.size += int64(h.Size)
					//fmt.Println("adding Block header # ", h.BlockNum, "to filemap at ", path)
				}

				//else it is a directory
				continue
			} else {

				//  if we are at file, create the map entry
				n := &filenode{partial, q, make([]*filenode, 1)}
				if partial == path {
					filemap[partial] = make(map[int][]BlockHeader)
					filemap[partial][h.BlockNum] = make([]BlockHeader, 1, 1)
					filemap[partial][h.BlockNum][0] = h
					dn.size += int64(h.Size)
					//fmt.fmt("creating Block header # ", h.BlockNum, "to filemap at ", path)
				}

				n.parent = q
				q.children = append(q.children, n)
				q = n
			}
		}
	}
	return nil
}

// AssignBlocks creates packets based on BlockHeader metadata and enqueues them for transmission
func AssignBlocks(bls []Block) {
	for _, b := range bls {
		AssignBlock(b)
	}
}

//bkdhash to get an single integer implicates the data
func BKD_Hash(c []byte) uint32 {
	var ans uint32 = 0
	length := len(c)
	var i int = 0
	for i < length {
		ans = ans*10 + (uint32)(c[i])
		i++
	}
	return ans
}


// AssignBlocks chooses a datanode which balances the load across nodes for a block and enqueues
// the block for distribution
func AssignBlock(b Block) (Packet, error) {
	p := new(Packet)

	if &b == nil || &b.Header == nil || &b.Data == nil || b.Header.Filename == "" ||
		b.Header.Size <= 0 || b.Header.BlockNum < 0 || b.Header.NumBlocks <= b.Header.BlockNum {
		return *p, errors.New("Invalid Block input")
	}

	if len(datanodemap) < 1 {
		return *p, errors.New("Cannot distribute Block, no datanodes are connected")
	}

	p.SRC = id
	p.CMD = BLOCK
	

	//Random load balancing
	nodeIDs := make([]string, len(datanodemap), len(datanodemap))
	i := 0
	for _, v := range datanodemap {
		if v.connected == false {
			continue
		}
		nodeIDs[i] = v.ID

		i++
	//	consistent.Add(v.ID)
	}
	//ToDo Hash  & backup in different node
	//	rand.Seed(time.Now().UTC().UnixNano())
	hashid := BKD_Hash(b.Data)
	nodeindex := (consistent).Search(hashid)
	//	nodeindex := rand.Intn(len(nodeIDs))
	p.DST = nodeIDs[(nodeindex+b.Header.Priority)%len(datanodemap)]
	b.Header.DatanodeID = p.DST

	p.Data = Block{b.Header, b.Data}

	return *p, nil

}

// SendPackets encodes packets and transmits them to their proper recipients
func SendPackets() {
	for p := range sendChannel {

		sendMapLock.Lock()
		encoder, ok := sendMap[p.DST]
		if !ok {
			fmt.Println("Could not find encoder for ", p.DST)
			continue
		}
		err := encoder.Encode(p)
		if err != nil {
			fmt.Println("Error sending", p.DST)
		}
		sendMapLock.Unlock()
	}
}

// WriteJSON writes a JSON encoded interface to disc
func WriteJSON(fileName string, key interface{}) {
	outFile, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error opening JSON ", err)
		return
	}
	defer outFile.Close()
	encoder := json.NewEncoder(outFile)
	err = encoder.Encode(key)
	if err != nil {
		fmt.Println("Error encoding JSON ", err)
		return
	}
}

func DeleteFiles(bh BlockHeader) (Packet, error) {
	p := new(Packet)

	if &p == nil {
		return *p, errors.New("Invalid BlockHeader input")
	}
	// Create Packet and delete block
	p.SRC = id
	p.CMD = DELETE
	p.DST = bh.DatanodeID
	headers := make([]BlockHeader, 1, 1)
	headers[0] = bh
	p.Headers = headers
	return *p, nil
}

// Handle handles a packet and performs the proper action based on its contents
func HandlePacket(p Packet) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic ", r)
			fmt.Println("Unable to handle packet ", p)
			return
		}
	}()

	if p.SRC == "" {
		fmt.Println("Could not identify packet")
		return
	}

	r := Packet{id, p.SRC, ACK, "", *new(Block), make([]BlockHeader, 0)}

	if p.SRC == "C" {

	swichCmd:
		switch p.CMD {
		case HB:
			fmt.Println("Received client connection", p.SRC)
			return
		case DIR:
			fmt.Println("Received Dir Request")
			r.Message = TraversalDir(p.Message)
			r.CMD = DIR
			fmt.Println(r)
		case LIST:
			fmt.Println("Received List Request")
			r.Message = ListFiles(p.Message)
			r.CMD = LIST
			fmt.Println(r)
		case DELETE:
			if p.Headers == nil || len(p.Headers) != 1 {
				r.CMD = ERROR
				r.Message = "Invalid Header received"
				fmt.Println("Invalid RemoveFiles Packet , ", p)
				break
			}
			headers := p.Headers
			bh := headers[0]
			filename := bh.Filename
			bkMap, ok := filemap[filename]
			if !ok {
				r.CMD = ERROR
				r.Message = "File not found " + filename
				fmt.Println("Requested file in filesystem not found, ", filename)
				break
			}

			_, ok = bkMap[0]
			if !ok {
				r.CMD = ERROR
				r.Message = "Could not locate first block in file"
				break
			}
			fmt.Println("Remove file(s) Request")

			for k, v := range filemap {
				if k == filename {
					for _, headers := range v {
						for _, vv := range headers {
							block := vv
							if !datanodemap[block.DatanodeID].connected {
								continue
							}
							tmp, err := DeleteFiles(block)
							if err != nil {
								r.CMD = ERROR
								r.Message = err.Error()
								break swichCmd
							}
							sendChannel <- tmp
							r.CMD = DELETE
							fmt.Println(r)
						}
					}
				} else {
					continue
				}
			}
			FSDelete(filename)
		case DISTRIBUTE:
			b := p.Data
			FSBuilding(b.Header.Filename)
			fmt.Println("Distributing Block ", b.Header.Filename, "/", b.Header.BlockNum, " to ", b.Header.DatanodeID)
			for i := 0; i < backups; i++ { //set backu-up
				b.Header.Priority = i
				p, err := AssignBlock(b)
				if err != nil {
					r.CMD = ERROR
					r.Message = err.Error()
					break swichCmd
				}
				sendChannel <- p
			}

			r.CMD = ACK
		case RETRIEVEBLOCK:
			r.CMD = RETRIEVEBLOCK
			if p.Headers == nil || len(p.Headers) != 1 {
				r.CMD = ERROR
				r.Message = "Invalid Header received"
				fmt.Println("Invalid RETRIEVEBLOCK Packet , ", p)
				break
			}

			r.DST = p.Headers[0].DatanodeID // Block to retrieve is specified by given header
			fmt.Println("Retrieving Block for client ", p.SRC, "from node ", r.DST)

			r.Headers = p.Headers
			// specify client that is requesting a block when it arrives
			clientMapLock.Lock()
			clientMap[p.Headers[0]] = p.SRC
			clientMapLock.Unlock()

		case GETHEADERS:
			r.CMD = GETHEADERS
			if p.Headers == nil || len(p.Headers) != 1 {
				r.CMD = ERROR
				r.Message = "Invalid Header received"
				fmt.Println("Received invalid Header Packet, ", p)
				break
			}

			fmt.Println("Retrieving headers for client using ", p.Headers[0])

			fname := p.Headers[0].Filename
			blockMap, ok := filemap[fname]
			if !ok {
				r.CMD = ERROR
				r.Message = "File not found " + fname
				fmt.Println("Requested file in filesystem not found, ", fname)
				break
			}

			_, ok = blockMap[0]
			if !ok {
				r.CMD = ERROR
				r.Message = "Could not locate first block in file"
				break
			}
			numBlocks := blockMap[0][0].NumBlocks

			headers := make([]BlockHeader, numBlocks, numBlocks)
		hs:
			for i, _ := range headers {

				_, ok = blockMap[i]
				if !ok {
					r.CMD = ERROR
					r.Message = "Could not find needed block in file "
					break
				}
				for j := 0; j < backups; j++ {
					if datanodemap[blockMap[i][j].DatanodeID].connected {
						headers[i] = blockMap[i][j] // grab the first available BlockHeader for each block number
						continue hs
					}
				}
				r.CMD = ERROR
				r.Message = "Could not find neededblock in file"
				break

			}
			r.Headers = headers
			fmt.Println("Retrieved headers ")
		}

	} else {
		dn := datanodemap[p.SRC]
		listed := dn.listed

		switch p.CMD {
		case HB:

			fmt.Println("Received Heartbeat from ", p.SRC)
			if !listed {
				r.CMD = LIST
			} else {
				r.CMD = ACK
			}

		case LIST:

			fmt.Println("Received BlockHeaders from ", p.SRC)
			list := p.Headers
			for _, h := range list {
				headerChannel <- h
			}
			dn.listed = true
			r.CMD = ACK

		case BLOCKACK:
			// receive acknowledgement for single Block header as being stored
			if p.Headers != nil && len(p.Headers) == 1 {
				headerChannel <- p.Headers[0]
			}
			r.CMD = ACK
			fmt.Println("Received BLOCKACK from ", p.SRC)

		case BLOCK:
			fmt.Println("Received Block Packet with header", p.Data.Header)

			// TODO map multiple clients
			//clientMapLock.Lock()
			//cID,ok := clientMap[p.Data.Header]
			//clientMapLock.Unlock()
			//if !ok {
			//	fmt.Println("Header not found in clientMap  ", p.Data.Header)
			//  return
			//}
			r.DST = "C"
			r.CMD = BLOCK
			r.Data = p.Data

		}
	}

	// send response
	sendChannel <- r

}

// Checkconnection adds or updates a connection to the namenode and handles its first packet
func CheckConnection(conn net.Conn, p Packet) {

	// C is the client(hardcode for now)
	if p.SRC == "C" {
		fmt.Println("Adding new client connection")
		sendMapLock.Lock()
		sendMap[p.SRC] = json.NewEncoder(conn)
		sendMapLock.Unlock()
	} else {
		dn, ok := datanodemap[p.SRC]
		if !ok {
			fmt.Println("Adding new datanode :", p.SRC)
			datanodemap[p.SRC] = &datanode{p.SRC, false, 0, true}
		} else {
			fmt.Printf("Datanode %s reconnected \n", dn.ID)
			dn.connected = true
		}
		sendMapLock.Lock()
		sendMap[p.SRC] = json.NewEncoder(conn)
		sendMapLock.Unlock()

		WriteToFile()

		dn = datanodemap[p.SRC]
		consistent.Add(dn.ID)
	}
	HandlePacket(p)
}
//get current's datanode and write their ID to file
func WriteToFile(){
		fmt.Println("ready to add here!!!")
//		fin,err:=os.Open(metadatapath+"/datanode1")
//		if err!=nil {
//			err=os.Remove(metadatapath+"/datanode1")
//			fin,err=os.Create(metadatapath+"/datanode1")
//		}
		cnt:=1
		var last string = ""
		for _,v:= range datanodemap {
			cnt++
			if v.connected==false {
				continue
			}
			tmp:=v.ID+"\r\n"
			last=last+tmp
			fmt.Println("write"+tmp)
//			fin.Write([]byte(tmp))
		}
		fmt.Println("last "+last)
		ioutil.WriteFile(metadatapath+"/datanode1",[]byte(last),0666)
		fmt.Println(cnt)
//		fin.Close()
}
// Handle Connection initializes the connection and performs packet retrieval
func HandleConnection(conn net.Conn) {

	// receive first Packet and add datanode if necessary
	var p Packet
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&p)
	if err != nil {
		fmt.Println("Unable to communicate with node")
	}
	CheckConnection(conn, p)
	dn := datanodemap[p.SRC]
	// receive packets and handle
	for {
		var p Packet
		err := decoder.Decode(&p)

		if err != nil {
			if dn == nil || dn.ID == "" {
				fmt.Println("Client disconnected!")
				return
			}
			fmt.Println("Datanode ", dn.ID, " disconnected!")
			dn.connected = false

			consistent.Remove(dn.ID)
			WriteToFile()
			return
		}
		HandlePacket(p)
	}
}

// listFiles is a recursive helper for ListFiles
func listFiles(node string, input string, depth int) string {

	pre := fsTree.Get(node)
	s, _ := pre.String()
	if s == "file" {
		input = strings.Repeat(" ", depth) + node + "\n"
		return input
	} else {
		input = strings.Repeat(" ", depth-1) + "-" + node + "\n"
		arr, _ := pre.Array()
		for _, v := range arr {
			input += listFiles(v.(string), input, depth+1)
		}
	}

	return input

}

// List the files of path
func ListFiles(path string) string {
	input := ""
	pre := fsTree.Get(path)
	s, _ := pre.String()
	if s == "file" {
		input = path + "\n"
	} else {
		input = path + "\n"
		arr, _ := pre.Array()
		for _, v := range arr {
			isFile, _ := fsTree.Get(v.(string)).String()
			paths := strings.Split(v.(string), "/")
			if isFile == "file" {
				input += "    " + paths[len(paths)-1] + "\n"
			} else {
				input += "    +" + paths[len(paths)-1] + "\n"
			}
		}
	}
	// input += listFiles("/", input, 1)
	return input
}

func traversalDir(node string, input string) string {
	pre := fsTree.Get(node)
	arr, _ := pre.Array()
	for _, v := range arr {
		s, _ := fsTree.Get(v.(string)).String()
		if s == "file" {
			input += "#" + v.(string)
		} else {
			input = traversalDir(v.(string), input)
		}

	}
	return input
}

func TraversalDir(path string) string {
	pre := fsTree.Get(path)
	if pre == nil {
		return ""
	}
	input := ""
	input += traversalDir(path, input)
	input = strings.TrimPrefix(input, "#")
	return input
}

// delete file in File System if it's updirectory is null delete it
func FSDelete(filename string) {
	paths := strings.Split(filename, "#")
	path := strings.Join(paths[1:len(paths)], "/")
	path = "/" + path

	delflag := false
	for i := len(paths) - 1; i >= 0; i-- {
		ParseCatalogue(metadatapath + "/meta")
		if i == len(paths)-1 {
			v := path
			_, isExist := fsTree.CheckGet(v)
			if isExist {
				fsTree.Del(v)
				delflag = true
				b, _ := fsTree.MarshalJSON()
				ioutil.WriteFile(metadatapath+"/meta", b, 0666)
			}
		} else {
			prepath := path
			path = strings.Join(paths[1:i+1], "/")
			path = "/" + path
			if delflag {
				subnode, _ := fsTree.Get(path).Array()
				var arr []string
				for _, x := range subnode {
					if x != prepath {
						arr = append(arr, x.(string))
					}
					fsTree.Set(path, arr)
					delflag = false
					if len(arr) == 0 && path != "/" {
						fsTree.Del(path)
						delflag = true
					}
					b, _ := fsTree.MarshalJSON()
					ioutil.WriteFile(metadatapath+"/meta", b, 0666)
				}
			}
		}
	}
}

// Add a new path for file to File System
func FSBuilding(filename string) {
	paths := strings.Split(filename, "#")
	fmt.Println(paths)
	changeflag := false
	var prepath string
	for k, v := range paths {
		if prepath != "" {
			if prepath == "/" {
				v = prepath + v
			} else {
				v = prepath + "/" + v
			}
		}
		_, isExist := fsTree.CheckGet(v)
		if !isExist {
			changeflag = true
			subnode, _ := fsTree.Get(prepath).Array()
			fsTree.Set(prepath, append(subnode, v))
			if k == len(paths)-1 {
				fsTree.Set(v, "file")
			} else {
				fsTree.Set(v, new([]string))
			}
		}
		prepath = v
	}
	if changeflag {
		b, _ := fsTree.MarshalJSON()
		fmt.Println(string(b))
		ioutil.WriteFile(metadatapath+"/meta", b, 0666)
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
		case "namenodeid":
			id = o.Value
		case "listenhost":
			host = o.Value
		case "listenport":
			port = o.Value
		case "sizeofblock":
			n, err := strconv.Atoi(o.Value)
			if err != nil {
				return err
			}

			if n < 4096 {
				return errors.New("Buffer size must be greater than or equal to 4096 bytes")
			}
			SIZEOFBLOCK = n
		case "metadatapath":
			metadatapath = o.Value
		case "backupnum":
			backups, _ = strconv.Atoi(o.Value)

		default:
			return errors.New("Bad ConfigOption received Key : " + o.Key + " Value : " + o.Value)
		}
	}

	return nil
}

// Parse the struct tree of file system
func ParseCatalogue(path string) error {
	b, _ := ioutil.ReadFile(path)
	var err error
	fsTree, err = simplejson.NewJson(b)
	if err == nil {

		s, _ := fsTree.MarshalJSON()
		fmt.Println(string(s))
	} else {
		log.Fatal("Fatal error ", err.Error())
	}
	return nil
}

// Init initializes all internal structures to run a namnode and handles incoming connections
func Init(configpath string) {

	// Read config
	err := ParseConfigXML(configpath)

	if err != nil {
		log.Fatal("Fatal error ", err.Error())
	}
	// setup filesystem
	root = &filenode{"/", nil, make([]*filenode, 0, 1)}
	filemap = make(map[string]map[int][]BlockHeader)
	err = os.Chdir(metadatapath)
	
	if os.IsNotExist(err) {
		os.Mkdir(metadatapath, 0700)
		os.Create(metadatapath + "/meta")
		os.Create(metadatapath + "/datanode")
		os.Create(metadatapath + "/datanode1")

		jsonStr := "{\"/\":[]}"
		fsTree, _ = simplejson.NewJson([]byte(jsonStr))
		b, _ := fsTree.MarshalJSON()
		ioutil.WriteFile(metadatapath+"/meta", b, 0666)
	} else {
		ParseCatalogue(metadatapath + "/meta")
	}
	fmt.Println("start init")
	
	consistent=NewConsisten()

//	fin,erro:=os.Open(metadatapath+"/datanode1")
//	fmt.Println(erro)
//	if erro!=nil {
//		fmt.Println("create new file")
//		fin,erro=os.Create(metadatapath+"/dataddnode1")
//	}
	
//	buf:=make([]byte,1024)
	buf,_:=ioutil.ReadFile(metadatapath+"/datanode1")
	name:=strings.Split(string(buf),"\n")
	length:=len(name)
	for i:=0;i<length-1;i++ {
		fmt.Println(name[i])
		consistent.Add(name[i])
	}
//	for{
//		n,_:=fin.Read(buf)
//		if n==0 {
//			break
//		}
//		name:=strings.Split(string(buf),"\n")
//		length:=len(name)
//		for i:=0;i<length-1;i++ {
//			fmt.Println(name[i])
//			consistent.Add(name[i])
//		}
//	}
//	fin.Close()


	
	// setup communication
	headerChannel = make(chan BlockHeader)
	sendChannel = make(chan Packet)
	sendMap = make(map[string]*json.Encoder)
	sendMapLock = sync.Mutex{}
	clientMap = make(map[BlockHeader]string)
	clientMapLock = sync.Mutex{}

	datanodemap = make(map[string]*datanode)

}

// Run starts the namenode
func Run(configpath string) {

	// setup filesystem
	Init(configpath)

	// Start communication
	go HandleBlockHeaders()
	go SendPackets()

	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Fatal("Fatal error ", err.Error())
	}

	// listen for datanode connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error ", err.Error())
			continue
		}
		go HandleConnection(conn)
	}
}
