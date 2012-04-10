#gobeanstalk
Go Beanstalkd client library

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
		fmt.Printf("id:%d, body:%s\n", j.Id, j.Body) // prints "hello"
		err = j.Delete()
		if err != nil {
			log.Fatal(err)
		}
    }
}
```

## Implementad command/API

* put
* reserve
* delete

## Author

* Iwan Budi Kusnanto
