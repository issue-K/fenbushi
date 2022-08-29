package main

import "fmt"

func main() {
	s := make([]int, 1)
	s[0] = 2
	w := s[0:0]
	fmt.Println(w)
	w = s[0:2]
	fmt.Println(w)
}
