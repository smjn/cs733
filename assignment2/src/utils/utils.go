// utils
package utils

import (
	"bytes"
	"encoding/gob"
)

type Command struct {
	Cmd []byte
	Val []byte
}

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

func (d *Command) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&d.Cmd)
	if err != nil {
		return err
	}
	return decoder.Decode(&d.Val)
}
