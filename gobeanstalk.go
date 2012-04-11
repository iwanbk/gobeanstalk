//Go beanstalkd client library
//Copyright(2012) Iwan Budi Kusnanto. See LICENSE for detail
package gobeanstalk

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
)

//beanstalkd error
var (
	errOutOfMemory    = errors.New("Out of Memory")
	errInternalError  = errors.New("Internal Error")
	errBadFormat      = errors.New("Bad Format")
	errUnknownCommand = errors.New("Unknown Command")
	errBuried         = errors.New("Buried")
	errExpectedCrlf   = errors.New("Expected CRLF")
	errJobTooBig      = errors.New("Job Too Big")
	errDraining       = errors.New("Draining")
	errDeadlineSoon   = errors.New("Deadline Soon")
	errTimedOut       = errors.New("Timed Out")
	errNotFound       = errors.New("Not Found")
	errUnknown        = errors.New("Unknown Error")
)

//Connection to beanstalkd
type Conn struct {
	conn   net.Conn
	addr   string
	reader *bufio.Reader
}

//create new connection
func NewConn(conn net.Conn, addr string) (*Conn, error) {
	c := new(Conn)
	c.conn = conn
	c.addr = addr
	c.reader = bufio.NewReader(conn)

	return c, nil
}


//A beanstalkd job
type Job struct {
	Id uint64
	Body []byte
}

//Create new job
func NewJob(id uint64, body []byte) *Job {
	j :=  &Job{id, body}
	return j
}

//Connect to beanstalkd server
func Dial(addr string) (*Conn, error) {
	kon, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c, err := NewConn(kon, addr)

	if err != nil {
		return nil, err
	}

	return c, nil
}

//Watching tube
func (c *Conn) Watch(tubename string) (int, error) {
	err := c.sendCmd("watch %s\r\n", tubename)
	if err != nil {
		return -1, err
	}

	//wait for response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		log.Println("[watch]waiting response failed:", err.Error())
		return -1, err
	}

	var tubeCount int
	_, err = fmt.Sscanf(resp, "WATCHING %d\r\n", &tubeCount)
	if err != nil {
		return -1, err
	}
	return tubeCount, nil
}

//Reserve Job
func (c *Conn) Reserve() (*Job, error) {
	//send command
	err := c.sendCmd("reserve\r\n")
	if err != nil {
		return nil, err
	}

	//wait for response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		log.Println("waiting response failed:", err.Error())
		return nil, err
	}

	//read response
	var id uint64
	var bodyLen int

	switch {
	case strings.Index(resp, "RESERVED") == 0:
		_, err = fmt.Sscanf(resp, "RESERVED %d %d\r\n", &id, &bodyLen)
		if err != nil {
			return nil, err
		}
	case resp == "DEADLINE_SOON\r\n":
		return nil, errDeadlineSoon
	case resp == "TIMED_OUT\r\n":
		return nil, errTimedOut
	}

	//read job body
	body, err := c.readBytes()
	if err != nil {
		log.Println("failed reading body:", err.Error())
		return nil, err
	}

	body = body[:len(body)-2]
	if len(body) != bodyLen {
		return nil, errors.New(fmt.Sprintf("invalid body len = %d/%d", len(body), bodyLen))
	}

	return &Job{id, body}, nil
}

//Delete a job
func (c *Conn) Delete(id uint64) error {
	err := c.sendCmd("delete %d\r\n", id)
	if err != nil {
		log.Println("send delete command failed:", err.Error())
		return err
	}

	//read response
	resp, err := c.reader.ReadString('\n')
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
	return parseCommonError(resp)
}

//Use tube
func (c *Conn) Use(tubename string) error {
	err := c.sendCmd("use %s\r\n", tubename)
	if err != nil {
		return err
	}

	//wait for response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		log.Println("[use]waiting response failed:", err.Error())
		return err
	}

	//match thw response
	expected := "USING " + tubename + "\r\n"
	if resp != expected {
		log.Println("response = ", resp)
		return errUnknown
	}
	return nil
}

//Put job
func (c *Conn) Put(data []byte, pri, delay, ttr int) (uint64, error) {
	header := fmt.Sprintf("put %d %d %d %d\r\n", pri, delay, ttr, len(data))
	cmd := concatSlice([]byte(header), data)
	cmd = concatSlice(cmd, []byte("\r\n"))
	_, err := c.conn.Write(cmd)
	if err != nil {
		log.Println("send job cmd failed")
		return 0, err
	}

	//read response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		log.Println("[put] response failed:", err.Error())
		return 0, err
	}

	//parse Put response
	switch {
	case strings.Index(resp, "INSERTED") == 0:
		var id uint64
		_, parseErr := fmt.Sscanf(resp, "INSERTED %d\r\n", &id)
		return id, parseErr
	case strings.Index(resp, "BURIED") == 0:
		var id uint64
		fmt.Sscanf(resp, "BURIED %d\r\n", &id)
		return id, errBuried
	case resp == "EXPECTED_CRLF\r\n":
		return 0, errExpectedCrlf
	case resp == "JOB_TOO_BIG\r\n":
		return 0, errJobTooBig
	case resp == "DRAINING\r\n":
		return 0, errDraining
	default:
		return 0, parseCommonError(resp)
	}
	return 0, errUnknown
}


/*
Release a job.

The release command puts a reserved job back into the ready queue (and marks
its state as "ready") to be run by any client. It is normally used when the job
fails because of a transitory error.
	id is the job id to release.
	pri is a new priority to assign to the job.
	delay is an integer number of seconds to wait before putting the job in
		the ready queue. The job will be in the "delayed" state during this time.
*/
func (c *Conn) Release(id uint64, pri, delay int) error {
	err := c.sendCmd("release %d %d %d\r\n", id, pri, delay)
	if err != nil {
		return err
	}

	//wait for response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		log.Println("[release]waiting response failed:", err.Error())
		return err
	}

	//match the response
	if resp == "RELEASED\r\n" {
		return nil
	}
	return parseCommonError(resp)
}

/*
Bury a job.

The bury command puts a job into the "buried" state. Buried jobs are put into a
FIFO linked list and will not be touched by the server again until a client
kicks them with the "kick" command.
	id is the job id to release.
	pri is a new priority to assign to the job.
*/
func (c *Conn) Bury(id uint64, pri int) error {
	err := c.sendCmd("bury %d %d\r\n", id, pri)
	if err != nil {
		return err
	}

	//wait for response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		log.Println("[release]waiting response failed:", err.Error())
		return err
	}

	//match the response
	if resp == "BURIED\r\n" {
		return nil
	}
	return parseCommonError(resp)

}

//Send command to server
func (c *Conn) sendCmd(format string, args ...interface{}) error {
	cmd := fmt.Sprintf(format, args...)
	_, err := c.conn.Write([]byte(cmd))
	if err != nil {
		log.Println("can't send to server :", err.Error())
	}
	return err
}

//read bytes until \n
func (c *Conn) readBytes() ([]byte, error) {
	rsp, err := c.reader.ReadBytes('\n')
	return rsp, err
}

//parse for Common Error
func parseCommonError(str string) error {
	switch str {
	case "BURIED\r\n":
		return errBuried
	case "NOT_FOUND\r\n":
		return errNotFound
	case "OUT_OF_MEMORY\r\n":
		return errOutOfMemory
	case "INTERNAL_ERROR\r\n":
		return errInternalError
	case "BAD_FORMAT\r\n":
		return errBadFormat
	case "UNKNOWN_COMMAND\r\n":
		return errUnknownCommand
	}
	return errUnknown
}

//concat two slices of []byte
func concatSlice(slc1, slc2 []byte) []byte {
	newSlc := make([]byte, len(slc1)+len(slc2))
	copy(newSlc, slc1)
	copy(newSlc[len(slc1):], slc2)
	return newSlc
}
