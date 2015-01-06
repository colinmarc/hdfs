package main

import (
	"encoding/hex"
	"fmt"
)

func checksum(paths []string) {
	expanded, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	for _, p := range expanded {
		reader, err := client.Open(p)
		if err != nil {
			fatal(err)
		}

		checksum, err := reader.Checksum()
		if err != nil {
			fatal(err)
		}

		fmt.Println(hex.EncodeToString(checksum), p)
	}
}
