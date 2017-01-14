package main

import (
	"fmt"
	"gopkg.in/redis.v5"
	"math/rand"
	"os"
	"time"
)

type connection *redis.Client
type roles int
type task string

const (
	master                 roles = iota
	slave                  roles = iota
	undefined              roles = iota
	masterKey                    = "somekeyword"
	masterExpire                 = time.Duration(5) * time.Second
	masterExtend                 = masterExpire / 2
	queueToProcess               = "queue1"
	queueErrors                  = "queue2"
	slaveWaitUntilElection       = time.Duration(7) * time.Second
	slaveMaxThreads              = 4
)

func main() {
	conn := connectRedis()

	for {
		switch chooseRole(conn) {
		case master:
			playAsMaster(conn)
		case slave:
			playAsSlave(conn)
		default:
			time.Sleep(time.Second)
		}
	}
}

func connectRedis() connection {
	opt := redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}

	conn := redis.NewClient(&opt)
	_, err := conn.Ping().Result()

	if err != nil {
		fmt.Println("Could not connect to redis", opt.Addr)
		os.Exit(-1)
	}
	return conn
}

func chooseRole(conn connection) roles {
	res, err := conn.SetNX(masterKey, "hereiam", masterExpire).Result()
	if err != nil {
		fmt.Println("chooseRole: error redis.setnx")
		return undefined
	}

	if res == true {
		return master
	}
	return slave
}

// MASTER PART
func playAsMaster(conn connection) {
	fmt.Println("play as master")
	master := state{true}

	go confirmMaster(conn, &master)

	for {
		if master.isAlive() == false {
			fmt.Println("playAsMaster: master died")
			return
		}

		if produceTask(conn) == false {
			fmt.Println("playAsMaster: produceTask error. die")
			master.die()
			return
		}

		time.Sleep(time.Millisecond * 500)
	}
}

func confirmMaster(conn connection, master *state) {
	for {
		if master.isAlive() == false {
			fmt.Println("confirmMaster: master died")
			return
		}

		start := time.Now()
		if extendMasterTime(conn) == false {
			fmt.Println("confirmMaster: error master extending. die")
			master.die()
			return
		}
		duration := time.Now().Unix() - start.Unix()

		// guard against network delays
		// if request take too much time, we can not be sure we are the master
		if duration >= int64(masterExtend.Seconds()) {
			fmt.Println("confirmMaster: too much time. die", duration)
			master.die()
			return
		}

		time.Sleep(masterExtend)
	}
}

func extendMasterTime(conn connection) bool {
	res, err := conn.Expire(masterKey, masterExpire).Result()
	if err != nil {
		fmt.Println("extendMasterTime: error redis.expire")
		return false
	}
	return res
}

func produceTask(conn connection) bool {
	message := "msg " + time.Now().String()
	res, err := conn.RPush(queueToProcess, message).Result()
	if err != nil {
		fmt.Println("extendMasterTime: error redis.expire")
		return false
	}
	fmt.Println("produceTask", message, res)
	return true
}

// SLAVE PART
func playAsSlave(conn connection) {
	fmt.Println("play as slave")
	slave := state{true}
	thread := make(chan int, slaveMaxThreads)
	errorTasks := &[]task{}

	for {
		if len(*errorTasks) > 0 {
			storeErrorTask(conn, errorTasks)
		}

		if slave.isAlive() == false {
			fmt.Println("playAsSlave: slave died")
			return
		}

		thread <- 1

		message, err := receiveTask(conn)
		if err != nil {
			slave.die()
			return
		}

		go proceedRecievedTask(message, thread, errorTasks)
	}
}

func storeErrorTask(conn connection, errorTasks *[]task) {
	fmt.Printf("storeErrorTask: len(%d)\n", len(*errorTasks))
	for _, task := range *errorTasks {
		_, err := conn.RPush(queueErrors, string(task)).Result()
		if err != nil {
			fmt.Printf("storeErrorTask: redis.rpush task(%s) err(%v)\n", task, err)
			return
		}
	}

	*errorTasks = []task{}
}

func receiveTask(conn connection) (task, error) {
	res, err := conn.BLPop(slaveWaitUntilElection, queueToProcess).Result()
	if err != nil {
		fmt.Printf("receiveTask: redis.blpop. Connection problem or master absent\n")
		return task(""), err
	}

	if res[0] != queueToProcess {
		msg := fmt.Sprintf("receiveTask: Wrong queue(%s) message:%s\n", res[0], res[1])
		panic(msg)
	}

	fmt.Printf("receiveTask: %s\n", res[1])
	return task(res[1]), nil
}

func proceedRecievedTask(message task, thread chan int, errorTasks *[]task) {
	fmt.Printf("proceedRecievedTask (%s)\n", message)
	if rand.Intn(100) <= 5 {
		*errorTasks = append(*errorTasks, message)
	}
	time.Sleep(time.Millisecond * 50)
	<-thread
}

// utils
type state struct {
	alive bool
}

func (s *state) isAlive() bool {
	return s.alive
}

func (s *state) die() {
	s.alive = false
}
