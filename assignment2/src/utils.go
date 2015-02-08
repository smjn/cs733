// utils
package utils

import (
	"encoding/gob"
	"fmt"
)

type Command struct {
	cmd []byte
	val []byte
}

func (d *Command) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(d.cmd)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(d.val)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (d *Command) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&d.cmd)
	if err != nil {
		return err
	}
	return decoder.Decode(&d.val)
}
