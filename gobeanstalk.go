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
)

//gobeanstalk error
var (
	errInvalidLen	  = errors.New("Invalid Length")
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
	cmd := fmt.Sprintf("watch %s\r\n", tubename)

	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return -1, err
	}

	var tubeCount int
	_, err = fmt.Sscanf(resp, "WATCHING %d\r\n", &tubeCount)
	if err != nil {
		return -1, parseCommonError(resp)
	}
	return tubeCount, nil
}

/*
Ignore tube.

The "ignore" command is for consumers. It removes the named tube from the
watch list for the current connection
*/
func (c *Conn) Ignore(tubename string) (int, error) {
	//send command and read response string
	cmd := fmt.Sprintf("ignore %s\r\n", tubename)
	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return -1, err
	}

	//parse response
	var tubeCount int
	_, err = fmt.Sscanf(resp, "WATCHING %d\r\n", &tubeCount)
	if err != nil {
		if resp == "NOT_IGNORED\r\n" {
			return -1, errors.New("Not Ignored")
		}
		return -1, parseCommonError(resp)
	}
	return tubeCount, nil
}

//Reserve Job
func (c *Conn) Reserve() (*Job, error) {
	//send command and read response
	resp, err := sendGetResp(c, "reserve\r\n")
	if err != nil {
		return nil, err
	}

	//parse response
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
	default:
		return nil, parseCommonError(resp)
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
	cmd := fmt.Sprintf("delete %d\r\n", id)
	expected := "DELETED\r\n"
	return sendExpectExact(c, cmd, expected)
}

/*
Use tube

The "use" command is for producers. Subsequent put commands will put jobs into
the tube specified by this command. If no use command has been issued, jobs
will be put into the tube named "default".
*/
func (c *Conn) Use(tubename string) error {
	//check parameter
	if len(tubename) > 200 {
		return errInvalidLen
	}

	cmd := fmt.Sprintf("use %s\r\n", tubename)
	expected := fmt.Sprintf("USING %s\r\n", tubename)
	return sendExpectExact(c, cmd, expected)
}

//Put job
func (c *Conn) Put(data []byte, pri, delay, ttr int) (uint64, error) {
	cmd := fmt.Sprintf("put %d %d %d %d\r\n", pri, delay, ttr, len(data))
	cmd = cmd + string(data) + "\r\n"

	resp, err := sendGetResp(c, cmd)
	if err != nil {
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
	cmd := fmt.Sprintf("release %d %d %d\r\n", id, pri, delay)
	expected := "RELEASED\r\n"
	return sendExpectExact(c, cmd, expected)
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
	cmd := fmt.Sprintf("bury %d %d\r\n", id, pri)
	expected := "BURIED\r\n"
	return sendExpectExact(c, cmd, expected)
}

/*
Touch a job

The "touch" command allows a worker to request more time to work on a job.
This is useful for jobs that potentially take a long time, but you still want
the benefits of a TTR pulling a job away from an unresponsive worker. A worker
may periodically tell the server that it's still alive and processing a job
(e.g. it may do this on DEADLINE_SOON)
*/
func (c *Conn) Touch(id uint64) error {
	cmd := fmt.Sprintf("touch %d\r\n", id)
	expected := "TOUCHED\r\n"
	return sendExpectExact(c, cmd, expected)
}

//send command and expect some exact response
func sendExpectExact(c *Conn, cmd, expected string) error {
	resp, err := sendGetResp(c, cmd)
	if err != nil {
		return err
	}

	if resp != expected {
		return parseCommonError(resp)
	}
	return nil
}

//Send command and read response
func sendGetResp(c *Conn, cmd string) (string, error) {
	_, err := c.conn.Write([]byte(cmd))
	if err != nil {
		return "", err
	}

	//wait for response
	resp, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return resp, nil
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
