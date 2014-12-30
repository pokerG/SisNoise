// Package common contains some common function,type and so on in SisNoise
package common

// commands for node communication
const (
	HB            = iota // heartbeat
	LIST          = iota // list directorys
	ACK           = iota // acknowledgement
	BLOCK         = iota // handle the incoming Block
	BLOCKACK      = iota // notifcation that Block was written to disc
	RETRIEVEBLOCK = iota // request to retrieve a Block
	DISTRIBUTE    = iota // request to distribute a Block to a datanode
	GETHEADERS    = iota // request to retrieve the headers of a given filename
	MKDIR         = iota // request create a directory
	ERROR         = iota // notification of a failed request
)

// The XML parsing structures for configuration options
type ConfigOptionList struct {
	XMLName       xml.Name       `xml:"ConfigOptionList"`
	ConfigOptions []ConfigOption `xml:"ConfigOption"`
}

type ConfigOption struct {
	Key   string `xml:"key,attr"`
	Value string `xml:",chardata"`
}

// A file is composed of one or more Blocks
type Block struct {
	Header BlockHeader // metadata
	Data   []byte      // data contents
}

// Blockheaders hold Block metadata
type BlockHeader struct {
	DatanodeID string // ID of datanode which holds the block
	Filename   string //the remote name of the block including the path "/test/0"
	Size       int64  // size of Block in bytes
	BlockNum   int    // the 0 indexed position of Block within file
	NumBlocks  int    // total number of Blocks in file
}

// Packets are sent over the network
type Packet struct {
	SRC     string        // source ID
	DST     string        // destination ID
	CMD     int           // command for the handler
	Message string        // optional packet contents explanation
	Data    Block         // optional Block
	Headers []BlockHeader // optional BlockHeader list
}
type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

// New returns an error that formats as the given text.
func New(text string) error {
	return &errorString{text}
}