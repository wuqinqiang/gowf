package gowf

import "fmt"

func Go(fn func()) {
	go safeGo(fn)
}

func safeGo(fn func()) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	fn()
}
