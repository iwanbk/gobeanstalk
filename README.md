#gobeanstalk [![Build Status](https://travis-ci.org/iwanbk/gobeanstalk.svg?branch=master)](https://travis-ci.org/iwanbk/gobeanstalk) [![GoDoc](https://godoc.org/github.com/iwanbk/gobeanstalk?status.svg)](https://godoc.org/github.com/iwanbk/gobeanstalk) 

Go [Beanstalkd](http://kr.github.io/beanstalkd/) client library.

## INSTALL
	go get github.com/iwanbk/gobeanstalk


## USAGE

### Producer
```go
import (
	"github.com/iwanbk/gobeanstalk"
	"log"
	"time"
)

func main() {
	conn, err := gobeanstalk.Dial("localhost:11300")
	if err != nil {
		log.Fatal(err)
	}

	id, err := conn.Put([]byte("hello"), 0, 10*time.Second, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Job id %d inserted\n", id)
}

```

### Consumer
```go
import (
	"github.com/iwanbk/gobeanstalk"
	"log"
)

func main() {
	conn, err := gobeanstalk.Dial("localhost:11300")
	if err != nil {
		log.Fatal(err)
	}
	for {
		j, err := conn.Reserve()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("id:%d, body:%s\n", j.ID, string(j.Body))
		err = conn.Delete(j.ID)
		if err != nil {
			log.Fatal(err)
		}
	}
}
```

## Implemented Commands

* use
* put
* watch
* ignore
* reserve
* delete
* touch
* release
* bury
* kick
* kick-job
* list-tubes
* stats
* stats-tube
* stats-job
* quit


# Release Notes
Latest release is v0.3 that contains API changes, see release notes [here](https://github.com/iwanbk/gobeanstalk/blob/master/ReleaseNotes.txt)

## Author

* [Iwan Budi Kusnanto](http://iwan.my.id)
