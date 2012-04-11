package gobeanstalk

import (
	"log"
)

//A beanstalkd job
type Job struct {
	Id uint64
	Body []byte
	c *Conn
}

//Create new job
func NewJob(id uint64, body []byte, c *Conn) *Job {
	j :=  &Job{id, body, c}
	return j
}

func (j *Job) Delete() error {
	err := j.c.sendCmd("delete %d\r\n", j.Id)
	if err != nil {
		log.Println("send delete command failed:", err.Error())
		return err
	}

	//read response
	resp, err := j.c.reader.ReadString('\n')
	if err != nil {
		log.Println("waiting response failed:", err.Error())
		return err
	}

	switch resp {
	case "DELETED\r\n":
		return nil
	case "NOT_FOUND\r\n":
		return errNotFound
	}
	return errUnknown
}
