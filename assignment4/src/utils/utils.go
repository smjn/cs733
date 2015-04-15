// utils
package utils

import (
	"bytes"
	"encoding/gob"
)

//Struct to help extraction of command and value from the Data field of raft.LogEntryData
type Command struct {
	Cmd []byte //the command like set .s..
	Val []byte //the value the user wants to send
}

//Custom encoder to encode the Command struct into a byte array. gob encoder will call it
//arguments: none
//returns: the byte array for encoded data, error
//receiver: pointer to Command struct
func (d *Command) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(d.Cmd)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(d.Val)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

//Custom decoder to decode a byte array with appr data to Command struct. gob decoder will call it.
//arguments: byte array with data to be decoded
//returns: error if any
//receiver: pointer to Command struct
func (d *Command) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&d.Cmd)
	if err != nil {
		return err
	}
	return decoder.Decode(&d.Val)
}
