package main

import (
	"fmt"
	"math"
	"time"
	"strconv"
	"sync"
	"math/rand"
)

const Threadhold = float64(0.0000001)

func Sqrt(x float64) float64 {
	var zn float64 = x
	var zn1 float64 = zn - (zn*zn-x)/(2*zn)
	for {
		t := math.Abs(zn1-zn)
		fmt.Println(t,zn,zn1)
		if t!=0.0 && t < Threadhold {
			break;
		}

		tmp := zn1
		zn1 = zn - (zn*zn-x)/(2*zn)
		zn = tmp
	}

	return zn1
}

func say(s string) {
	wg.Add(1)
	defer wg.Done()
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		fmt.Println(strconv.Itoa(i)+":"+s)
	}
}

var wg sync.WaitGroup

func main() {
	timer := time.NewTimer(1000*time.Millisecond)
	for {
		select {
		case <- timer.C:
			fmt.Println("hello, on tick!")
			timer.Reset(1000*time.Millisecond)
		}
	}
	for i :=0;i<100000000000;i++{
		fmt.Println("--")
	}

	fmt.Println("exit!")
}


func randInt(min int, max int) int {

	return min + rand.Intn(max-min)
}