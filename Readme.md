#gobeanstalk
Go [Beanstalkd](http://kr.github.com/beanstalkd/) client library.
Read the doc [here](http://go.pkgdoc.org/github.com/iwanbk/gobeanstalk) .

## INSTALL
	go get github.com/iwanbk/gobeanstalk


## USAGE

### Producer
```go
import (
	"fmt"
	"github.com/iwanbk/gobeanstalk"
	"log"
)

func main() {
    conn, err := gobeanstalk.Dial("localhost:11300")
	if err != nil {
		log.Fatal(err)
	}
	id, err := conn.Put([]byte("hello"), 0, 0, 10)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Job id %d inserted\n", id)
}
```

### Consumer
```go
import (
	"github.com/iwanbk/gobeanstalk"
	"fmt"
	"log"
)

func main() {
	conn, err := gobeanstalk.Dial("localhost:11300")
	if err != nil {
		log.Printf("connect failed")
		log.Fatal(err)
	}
    for {
        j, err := conn.Reserve()
		if err != nil {
			log.Println("reserve failed")
			log.Fatal(err)
		}
		fmt.Printf("id:%d, body:%s\n", j.Id, string(j.Body))
		err = conn.Delete(j.Id)
		if err != nil {
			log.Fatal(err)
		}
    }
}
```

## Implemented Commands

Producer commands:

* use
* put

Worker commands:

* watch
* ignore
* reserve
* delete
* touch
* release
* bury

## Author

* [Iwan Budi Kusnanto](http://ibk.labhijau.net)
